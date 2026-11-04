//! rescue-server

mod hooks;
mod hub;
mod inner;
mod protocol;
mod pty_ws;
mod rescue_ws;

use axum::routing::get;
use inner::{Inner, TicketError};
use std::sync::Arc;
use uuid::Uuid;

pub use hooks::{AuditEvent, EndReason, Hooks, RecordingOutcome, SessionEnd, SessionStart};
pub use inner::Config;
pub use protocol::Envelope;

pub struct RescueServer {
    inner: Arc<Inner>,
}

impl RescueServer {
    pub fn new(hooks: Arc<dyn Hooks>, cfg: Config) -> Self {
        Self {
            inner: Arc::new(Inner {
                cfg,
                hooks,
                rescue_hub: Default::default(),
                pty_sessions: Default::default(),
                tickets: Default::default(),
            }),
        }
    }

    pub fn router(&self) -> axum::Router {
        axum::Router::new()
            .route("/ws/rescue", get(rescue_ws::ws_handler))
            .route("/ws/pty/:device_id", get(pty_ws::ws_handler))
            .with_state(self.inner.clone())
    }

    pub fn issue_ticket(&self, user_id: Uuid, device_id: Uuid) -> Result<(Uuid, u64), TicketError> {
        self.inner.issue_ticket(user_id, device_id)
    }

    pub fn rescue_online(&self, device_id: Uuid) -> bool {
        self.inner.rescue_hub.connected(device_id)
    }

    pub fn open_sessions(&self, device_id: Uuid) -> usize {
        self.inner
            .pty_sessions
            .iter()
            .filter(|e| e.value().device_id == device_id)
            .count()
    }
}
