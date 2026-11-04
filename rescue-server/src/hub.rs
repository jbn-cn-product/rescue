//! 设备通道注册表

use crate::protocol::Envelope;
use dashmap::DashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

pub(crate) enum Outbound {
    Frame(Envelope),
    Close(u16),
}

#[derive(Clone, Default)]
pub(crate) struct Hub(DashMap<Uuid, mpsc::Sender<Outbound>>);

impl Hub {
    /// 插入新连接；若旧连接存在，返回其发送
    pub(crate) fn insert(
        &self,
        id: Uuid,
        tx: mpsc::Sender<Outbound>,
    ) -> Option<mpsc::Sender<Outbound>> {
        self.0.insert(id, tx)
    }

    pub(crate) fn remove_mine(&self, id: Uuid, tx: &mpsc::Sender<Outbound>) -> bool {
        self.0.remove_if(&id, |_, v| v.same_channel(tx)).is_some()
    }

    pub(crate) fn connected(&self, id: Uuid) -> bool {
        self.0.contains_key(&id)
    }

    pub(crate) async fn send(&self, id: Uuid, env: Envelope) -> bool {
        match self.0.get(&id) {
            Some(tx) => tx.send(Outbound::Frame(env)).await.is_ok(),
            None => false,
        }
    }
}
