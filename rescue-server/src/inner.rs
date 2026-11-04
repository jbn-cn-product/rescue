//! crate 私有运行时状态
use crate::hooks::Hooks;
use crate::hub::Hub;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

/// 协议参数
#[derive(Clone)]
pub struct Config {
    /// 一次性 ticket 的有效期
    pub ticket_ttl: Duration,
    pub idle_timeout: Duration,
    pub record_max_bytes: usize,
    pub ping_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ticket_ttl: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(600),
            record_max_bytes: 8 * 1024 * 1024,
            ping_interval: Duration::from_secs(30),
        }
    }
}

pub(crate) struct Inner {
    pub cfg: Config,
    pub hooks: Arc<dyn Hooks>,
    pub rescue_hub: Hub,
    pub pty_sessions: DashMap<Uuid, PtyChannel>,
    pub tickets: DashMap<Uuid, Ticket>,
}

pub(crate) struct PtyChannel {
    pub device_id: Uuid,
    pub tx: mpsc::Sender<PtyEvent>,
}

pub(crate) enum PtyEvent {
    Data(Vec<u8>),
    Exit(i32),
    DeviceOffline,
}

pub(crate) struct Ticket {
    pub user_id: Uuid,
    pub device_id: Uuid,
    pub expires_at: Instant,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TicketError {
    DeviceOffline,
}

impl Inner {
    pub(crate) fn issue_ticket(
        &self,
        user_id: Uuid,
        device_id: Uuid,
    ) -> Result<(Uuid, u64), TicketError> {
        if !self.rescue_hub.connected(device_id) {
            return Err(TicketError::DeviceOffline);
        }
        let now = Instant::now();
        self.tickets.retain(|_, t| t.expires_at > now);
        let ticket = Uuid::new_v4();
        self.tickets.insert(
            ticket,
            Ticket {
                user_id,
                device_id,
                expires_at: now + self.cfg.ticket_ttl,
            },
        );
        Ok((ticket, self.cfg.ticket_ttl.as_secs()))
    }

    pub(crate) fn consume_ticket(&self, ticket: &str, device_id: Uuid, user_id: Uuid) -> bool {
        let Some(t) = Uuid::parse_str(ticket).ok() else {
            return false;
        };
        self.tickets
            .remove_if(&t, |_, t| {
                t.device_id == device_id && t.user_id == user_id && t.expires_at > Instant::now()
            })
            .is_some()
    }

    pub(crate) async fn on_device_offline(&self, device_id: Uuid) {
        let txs: Vec<_> = self
            .pty_sessions
            .iter()
            .filter(|e| e.value().device_id == device_id)
            .map(|e| e.value().tx.clone())
            .collect();
        for tx in txs {
            let _ = tx.send(PtyEvent::DeviceOffline).await;
        }
    }

    pub(crate) fn session_of_device(&self, device_id: Uuid) -> Option<Uuid> {
        self.pty_sessions
            .iter()
            .find(|e| e.value().device_id == device_id)
            .map(|e| *e.key())
    }
}
