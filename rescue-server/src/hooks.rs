//! 产品对接 Hook：
//!
//! 两个鉴权 Hook 的错误类
//! - `auth_device` 的失败发生在 WS upgrade **之后** → 用 close code；
//! - `auth_browser` 的失败发生在 upgrade **之前** → 用 HTTP 状态码。

use axum::http::{HeaderMap, StatusCode};
use uuid::Uuid;

/// 会话开始
pub struct SessionStart {
    pub session_id: Uuid,
    pub device_id: Uuid,
    pub user_id: Uuid,
}

/// 会话结束
pub struct SessionEnd {
    pub session_id: Uuid,
    pub device_id: Uuid,
    pub user_id: Uuid,
    pub reason: EndReason,
    pub recording: RecordingOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    UserClose,
    BrowserClosed,
    DeviceOffline,
    IdleTimeout,
    ShellExit,
    Internal,
}

impl EndReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserClose => "user_close",
            Self::BrowserClosed => "browser_closed",
            Self::DeviceOffline => "device_offline",
            Self::IdleTimeout => "idle_timeout",
            Self::ShellExit => "shell_exit",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingOutcome {
    NotRecorded,
    Saved { key: String, truncated: bool },
    Lost { bytes: usize, truncated: bool },
}

pub struct AuditEvent {
    pub action: &'static str,
    pub user_id: Uuid,
    pub device_id: Uuid,
    pub session_id: Uuid,
}

#[async_trait::async_trait]
pub trait Hooks: Send + Sync + 'static {
    async fn auth_device(&self, token: &str) -> Result<Uuid, u16>;

    async fn auth_browser(&self, headers: &HeaderMap) -> Result<Uuid, StatusCode>;

    async fn session_started(&self, _s: &SessionStart) {}

    async fn session_ended(&self, _s: &SessionEnd) {}

    async fn save_recording(
        &self,
        _session_id: Uuid,
        _bytes: &[u8],
        _truncated: bool,
    ) -> RecordingOutcome {
        RecordingOutcome::NotRecorded
    }

    async fn audit(&self, _e: &AuditEvent) {}
}
