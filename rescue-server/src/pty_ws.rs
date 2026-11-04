//! 浏览器通道 `/ws/pty/:device_id`
//! Cookie JWT + dao + storage + audit → Hooks。

use crate::hooks::{AuditEvent, EndReason, RecordingOutcome, SessionEnd, SessionStart};
use crate::inner::{Inner, PtyEvent};
use crate::protocol::Envelope;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use uuid::Uuid;

/// 等浏览器 xterm 首帧 resize 再开 pty
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
pub(crate) struct PtyQuery {
    ticket: Option<String>,
}

pub(crate) async fn ws_handler(
    State(state): State<Arc<Inner>>,
    ws: WebSocketUpgrade,
    Path(device_id): Path<Uuid>,
    headers: axum::http::HeaderMap,
    Query(q): Query<PtyQuery>,
) -> Response {
    let user_id = match state.hooks.auth_browser(&headers).await {
        Ok(u) => u,
        Err(code) => return code.into_response(),
    };

    if !state.consume_ticket(q.ticket.as_deref().unwrap_or(""), device_id, user_id) {
        return (
            StatusCode::from_u16(423).unwrap(),
            axum::Json(serde_json::json!({
                "error": { "code": "ticket_invalid", "message": "票据无效或已过期" }
            })),
        )
            .into_response();
    }

    if !state.rescue_hub.connected(device_id) {
        return (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "error": { "code": "device_offline", "message": "设备终端通道不在线" }
            })),
        )
            .into_response();
    }
    if state.session_of_device(device_id).is_some() {
        return (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "error": { "code": "session_exists", "message": "该设备已有终端会话" }
            })),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| async move {
        session(state, socket, device_id, user_id).await;
    })
}

enum BrowserMsg {
    Input(Vec<u8>),
    Resize { rows: u16, cols: u16 },
    Close,
}

fn parse_browser(text: &str) -> Option<BrowserMsg> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    match v["type"].as_str()? {
        "input" => Some(BrowserMsg::Input(B64.decode(v["data"].as_str()?).ok()?)),
        "resize" => Some(BrowserMsg::Resize {
            rows: v["rows"].as_u64()?.clamp(2, 200) as u16,
            cols: v["cols"].as_u64()?.clamp(10, 500) as u16,
        }),
        "close" => Some(BrowserMsg::Close),
        _ => None,
    }
}

async fn session(state: Arc<Inner>, socket: WebSocket, device_id: Uuid, user_id: Uuid) {
    let (mut sender, mut receiver) = socket.split();
    let session_id = Uuid::new_v4();

    let (rows, cols, first) = match tokio::time::timeout(FIRST_FRAME_TIMEOUT, receiver.next()).await
    {
        Ok(Some(Ok(Message::Text(t)))) => match parse_browser(&t) {
            Some(BrowserMsg::Resize { rows, cols }) => (rows, cols, None),
            m @ Some(_) => (24, 80, m),
            None => (24, 80, None),
        },
        _ => (24, 80, None),
    };

    if state.session_of_device(device_id).is_some() {
        let _ = sender
            .send(Message::Text(
                serde_json::json!({ "type": "closed", "reason": "session_exists" }).to_string(),
            ))
            .await;
        return;
    }

    let (tx, mut rx) = mpsc::channel::<PtyEvent>(256);
    state
        .pty_sessions
        .insert(session_id, crate::inner::PtyChannel { device_id, tx });
    let opened = state
        .rescue_hub
        .send(
            device_id,
            Envelope::new(
                "pty.open",
                serde_json::json!({ "session_id": session_id, "rows": rows, "cols": cols }),
            ),
        )
        .await;
    if !opened {
        state.pty_sessions.remove(&session_id);
        let _ = sender
            .send(Message::Text(
                serde_json::json!({ "type": "closed", "reason": "device_offline" }).to_string(),
            ))
            .await;
        return;
    }
    state
        .hooks
        .session_started(&SessionStart {
            session_id,
            device_id,
            user_id,
        })
        .await;
    state
        .hooks
        .audit(&AuditEvent {
            action: "pty.open",
            user_id,
            device_id,
            session_id,
        })
        .await;
    tracing::info!(%session_id, %device_id, rows, cols, "pty session open");

    if let Some(BrowserMsg::Input(b)) = first {
        state
            .rescue_hub
            .send(
                device_id,
                Envelope::new(
                    "pty.input",
                    serde_json::json!({
                        "session_id": session_id, "data": B64.encode(&b),
                    }),
                ),
            )
            .await;
    }

    let mut record: Vec<u8> = Vec::new();
    let mut record_truncated = false;

    let idle = tokio::time::sleep(state.cfg.idle_timeout);
    tokio::pin!(idle);

    let mut reason = EndReason::ShellExit;
    'outer: loop {
        tokio::select! {

            _ = &mut idle => { reason = EndReason::IdleTimeout; break; }
            ev = rx.recv() => match ev {
                Some(PtyEvent::Data(b)) => {
                    push_record(&mut record, &mut record_truncated, &state.cfg, &b);
                    if sender
                        .send(Message::Text(
                            serde_json::json!({ "type": "data", "data": B64.encode(&b) }).to_string(),
                        ))
                        .await
                        .is_err()
                    {
                        reason = EndReason::BrowserClosed; break;
                    }
                }
                Some(PtyEvent::Exit(code)) => {
                    let _ = sender
                        .send(Message::Text(
                            serde_json::json!({ "type": "exit", "code": code }).to_string(),
                        ))
                        .await;
                    break; // reason 已默认 ShellExit
                }
                Some(PtyEvent::DeviceOffline) => {
                    reason = EndReason::DeviceOffline; break;
                }
                None => { reason = EndReason::Internal; break; }
            },
            msg = receiver.next() => match msg {
                Some(Ok(Message::Text(t))) => match parse_browser(&t) {
                    Some(BrowserMsg::Input(b)) => {
                        push_record(&mut record, &mut record_truncated, &state.cfg, &b);
                        state.rescue_hub.send(
                            device_id,
                            Envelope::new("pty.input", serde_json::json!({
                                "session_id": session_id, "data": B64.encode(&b),
                            })),
                        ).await;
                    }
                    Some(BrowserMsg::Resize { rows, cols }) => {
                        state.rescue_hub.send(
                            device_id,
                            Envelope::new("pty.resize", serde_json::json!({
                                "session_id": session_id, "rows": rows, "cols": cols,
                            })),
                        ).await;
                    }
                    Some(BrowserMsg::Close) => { reason = EndReason::UserClose; break 'outer; }
                    None => {}
                },

                Some(Ok(_)) => {}
                Some(Err(_)) | None => { reason = EndReason::BrowserClosed; break; }
            },
        }
        idle.as_mut().reset(Instant::now() + state.cfg.idle_timeout);
    }

    state
        .rescue_hub
        .send(
            device_id,
            Envelope::new("pty.close", serde_json::json!({ "session_id": session_id })),
        )
        .await;
    state.pty_sessions.remove(&session_id);

    let recording = if record.is_empty() {
        RecordingOutcome::NotRecorded
    } else {
        state
            .hooks
            .save_recording(session_id, &record, record_truncated)
            .await
    };
    state
        .hooks
        .session_ended(&SessionEnd {
            session_id,
            device_id,
            user_id,
            reason,
            recording,
        })
        .await;
    state
        .hooks
        .audit(&AuditEvent {
            action: "pty.close",
            user_id,
            device_id,
            session_id,
        })
        .await;
    tracing::info!(%session_id, %device_id, reason = reason.as_str(), "pty session end");

    // ⑤
    let _ = sender
        .send(Message::Text(
            serde_json::json!({ "type": "closed", "reason": reason.as_str() }).to_string(),
        ))
        .await;
    let _ = sender.send(Message::Close(None)).await;
}

/// 超上限截断
fn push_record(
    record: &mut Vec<u8>,
    truncated: &mut bool,
    cfg: &crate::inner::Config,
    bytes: &[u8],
) {
    if *truncated {
        return;
    }
    if record.len() + bytes.len() > cfg.record_max_bytes {
        let room = cfg.record_max_bytes.saturating_sub(record.len());
        record.extend_from_slice(&bytes[..room]);
        *truncated = true;
    } else {
        record.extend_from_slice(bytes);
    }
}
