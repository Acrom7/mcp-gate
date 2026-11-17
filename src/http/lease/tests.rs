use super::*;
use tokio::sync::Semaphore;

fn lease() -> (Arc<Semaphore>, LeaseRef) {
    let capacity = Arc::new(Semaphore::new(1));
    let lease = Arc::new(Mutex::new(Lease {
        streams: 0,
        requests: 0,
        disconnected: None,
        last_activity: Instant::now() - Duration::from_secs(30),
        closing: false,
        active: 0,
        _capacity: capacity.clone().try_acquire_owned().unwrap(),
    }));
    (capacity, lease)
}

#[test]
fn completed_discovery_does_not_extend_idle_but_executable_completion_does() {
    let (_, lease) = lease();
    let before = lease.lock().unwrap().last_activity;
    lease.lock().unwrap().requests = 1;
    drop(ConnectionGuard {
        lease: lease.clone(),
        stream: false,
    });
    assert_eq!(lease.lock().unwrap().last_activity, before);
    assert_eq!(
        lease
            .lock()
            .unwrap()
            .expiry(Some(Duration::from_secs(10)), Duration::from_secs(60)),
        Some("idle")
    );
    let activity = Activity(lease.clone());
    let running = activity.start();
    let queued = activity.start();
    assert!(lease.lock().unwrap().last_activity > before);
    assert_eq!(
        lease
            .lock()
            .unwrap()
            .expiry(Some(Duration::ZERO), Duration::ZERO),
        None
    );
    drop(running);
    assert_eq!(lease.lock().unwrap().active, 1);
    drop(queued);
    assert_eq!(lease.lock().unwrap().active, 0);
    assert_eq!(
        lease
            .lock()
            .unwrap()
            .expiry(Some(Duration::from_secs(10)), Duration::from_secs(60)),
        None
    );
}

#[test]
fn open_get_stream_protects_recordings_and_closure_refreshes_idle() {
    let (_, lease) = lease();
    lease.lock().unwrap().streams = 1;
    assert_eq!(
        lease
            .lock()
            .unwrap()
            .expiry(Some(Duration::ZERO), Duration::ZERO),
        None
    );
    let before = lease.lock().unwrap().last_activity;
    drop(ConnectionGuard {
        lease: lease.clone(),
        stream: true,
    });
    let lease = lease.lock().unwrap();
    assert!(lease.last_activity > before);
    assert_eq!(lease.streams, 0);
    assert!(lease.disconnected.is_some());
    assert_eq!(
        lease.expiry(Some(Duration::from_secs(10)), Duration::from_secs(60)),
        None
    );
}

#[test]
fn disabled_idle_preserves_disconnect_grace_and_handler_capacity_ownership() {
    let (capacity, lease) = lease();
    assert_eq!(lease.lock().unwrap().expiry(None, Duration::ZERO), None);
    lease.lock().unwrap().disconnected = Some(Instant::now() - Duration::from_secs(30));
    assert_eq!(
        lease.lock().unwrap().expiry(None, Duration::from_secs(10)),
        Some("disconnected")
    );
    let activity = Activity(lease.clone());
    let running = activity.start();
    drop(activity);
    drop(lease);
    assert_eq!(capacity.available_permits(), 0);
    drop(running);
    assert_eq!(capacity.available_permits(), 1);
}
