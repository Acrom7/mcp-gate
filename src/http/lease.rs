//! HTTP session leases own capacity until response bodies and executable
//! handlers finish, even if the SDK removes the session during disconnect.
use super::HttpState;
use crate::config::Ownership;
use axum::body::{Body, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use rmcp::transport::streamable_http_server::session::{
    SessionManager, local::LocalSessionManager,
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
mod tests;

pub(super) struct Lease {
    pub streams: usize,
    pub requests: usize,
    pub disconnected: Option<Instant>,
    pub last_activity: Instant,
    pub closing: bool,
    pub active: usize,
    pub _capacity: OwnedSemaphorePermit,
}
pub(super) type LeaseRef = Arc<Mutex<Lease>>;

impl Lease {
    fn expiry(&self, idle: Option<Duration>, grace: Duration) -> Option<&'static str> {
        if self.closing || self.streams > 0 || self.requests > 0 || self.active > 0 {
            return None;
        }
        if self.disconnected.is_some_and(|t| t.elapsed() >= grace) {
            Some("disconnected")
        } else if idle.is_some_and(|idle| self.last_activity.elapsed() >= idle) {
            Some("idle")
        } else {
            None
        }
    }
}

pub(super) async fn reap_sessions(state: &HttpState, manager: &LocalSessionManager) {
    let idle = match state.gateway.config.ownership {
        Ownership::Session => state.gateway.config.idle_timeout(),
        Ownership::Shared => None,
    };
    let grace = Duration::from_secs(state.gateway.config.disconnect_grace_seconds);
    // Mark closing while holding the lease lock, excluding new HTTP admissions.
    // Release all blocking locks before SDK cancellation and backend cleanup.
    let expired: Vec<_> = state
        .leases
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(id, lease)| {
            let mut lease = lease.lock().unwrap();
            let reason = lease.expiry(idle, grace)?;
            lease.closing = true;
            Some((id.clone(), reason))
        })
        .collect();
    for (id, reason) in expired {
        if manager.close_session(&id.clone().into()).await.is_err() {
            tracing::warn!("expired HTTP session could not be closed by the SDK");
        }
        state.leases.lock().unwrap().remove(&id);
        tracing::info!(reason, "expired HTTP session closed");
    }
}

pub(super) struct ConnectionGuard {
    pub lease: LeaseRef,
    pub stream: bool,
}
#[derive(Clone)]
pub(crate) struct Activity(pub(super) LeaseRef);
impl Activity {
    pub fn start(&self) -> ActiveCall {
        let mut lease = self.0.lock().unwrap();
        lease.active += 1;
        lease.last_activity = Instant::now();
        ActiveCall(self.0.clone())
    }
}
// The lease also owns its session-capacity permit. Cancellation may outlive the
// HTTP response; keep that permit and block expiry through handler completion.
pub(crate) struct ActiveCall(LeaseRef);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        let mut lease = self.0.lock().unwrap();
        lease.last_activity = Instant::now();
        lease.active -= 1;
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let mut value = self.lease.lock().unwrap();
        if self.stream {
            // A GET stream protects session-owned recordings. Once the last
            // stream closes, allow a full idle interval to reconnect.
            value.last_activity = Instant::now();
            value.streams -= 1;
            if value.streams == 0 {
                value.disconnected = Some(Instant::now());
            }
        } else {
            value.requests -= 1;
            if value.disconnected.is_some() {
                value.disconnected = Some(Instant::now());
            }
        }
    }
}
pub(super) struct TrackedBody {
    pub body: Body,
    pub _guard: Option<ConnectionGuard>,
}
impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
