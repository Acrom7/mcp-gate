#![cfg(feature = "test-backend")]
#[allow(
    dead_code,
    reason = "Shared integration fixtures are exercised by different suites"
)]
mod support;
use reqwest::{Method, StatusCode};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;

#[tokio::test]
async fn discovery_and_ping_do_not_keep_session_backend_alive() {
    let mut h = Harness::session_limits(1, 1, 1).await;
    let a = h.session().await;
    a.call("state", json!({})).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut closed = false;
    while tokio::time::Instant::now() < deadline {
        for method in ["tools/list", "resources/list", "prompts/list", "ping"] {
            let response = a
                .post(json!({"jsonrpc":"2.0","id":100,"method":method,"params":{}}))
                .await;
            if response.status() == StatusCode::NOT_FOUND {
                closed = true;
                break;
            }
            assert!(response.status().is_success());
            a.read_message(response, Some(100)).await;
        }
        if closed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        closed,
        "Discovery and ping extended the session idle deadline"
    );
    h.workers(0).await;
    h.stop();
}

#[tokio::test]
async fn every_executable_operation_extends_session_idle_from_completion() {
    let mut h = Harness::session_limits(1, 3, 1).await;
    let a = h.session().await;
    a.call("state", json!({"value":"kept"})).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let resource = a
        .request("resources/read", json!({"uri":"fixture://first"}))
        .await;
    assert!(resource["result"].is_object());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let prompt = a.request("prompts/get", json!({"name":"echo"})).await;
    assert!(prompt["result"].is_object());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        mock_value(&a.call("state", json!({})).await)["value"],
        "kept"
    );
    h.workers(1).await;
    h.workers(0).await;
    h.stop();
}

#[tokio::test]
async fn disabled_session_idle_preserves_state_and_capacity_eviction() {
    let mut h = Harness::session_limits(1, 0, 1).await;
    let a = h.session().await;
    a.call("state", json!({"value":"old"})).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    h.workers(1).await;
    a.request("ping", json!({})).await;
    let b = h.session().await;
    assert_eq!(
        mock_value(&b.call("state", json!({"value":"new"})).await)["value"],
        "new"
    );
    assert!(content(&a.call("state", json!({})).await).contains("Reconnect"));
    h.workers(1).await;
    b.close().await;
    h.workers(0).await;
    h.stop();
}

#[tokio::test]
async fn open_get_stream_keeps_session_backend_alive_beyond_idle_timeout() {
    let mut h = Harness::session_limits(1, 1, 1).await;
    let a = h.session().await;
    let stream = a.req(Method::GET).send().await.unwrap();
    assert!(stream.status().is_success());
    a.call("state", json!({"value":"recording"})).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    h.workers(1).await;
    assert_eq!(
        mock_value(&a.call("state", json!({})).await)["value"],
        "recording"
    );
    drop(stream);
    h.workers(0).await;
    h.stop();
}

#[tokio::test]
async fn session_active_and_queued_calls_survive_idle_deadline() {
    let mut h = Harness::session_limits(1, 1, 1).await;
    let a = h.session().await;
    let caller = a.clone();
    let running = tokio::spawn(async move {
        caller
            .request(
                "tools/call",
                json!({"name":"pause", "arguments":{"delay_ms":2500},
            "_meta":{"progressToken":"test-progress"}}),
            )
            .await
    });
    for _ in 0..100 {
        if a.progress.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(a.progress.load(Ordering::SeqCst) > 0);
    let queued = tokio::spawn(async move { a.call("state", json!({"value":"queued"})).await });
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(!running.is_finished());
    assert!(!queued.is_finished());
    h.workers(1).await;
    assert_eq!(content(&running.await.unwrap()), "done");
    assert_eq!(mock_value(&queued.await.unwrap())["value"], "queued");
    h.workers(0).await;
    h.stop();
}
