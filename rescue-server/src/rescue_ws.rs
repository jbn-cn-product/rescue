//! 设备通道 `/ws/rescue`
//! state → Inner → dao/audit → Hooks。

use crate::hub::Outbound;
use crate::inner::{Inner, PtyEvent};
use crate::protocol::Envelope;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

pub(crate) async fn ws_handler(
    State(state): State<Arc<Inner>>,
    ws: WebSocketUpgrade,
    headers: HeaderMap,
) -> Response {
    // Bearer 头
    let token = bearer(&headers).unwrap_or_default();
    let auth = state.hooks.auth_device(&token).await;
    ws.on_upgrade(move |socket| async move {
        match auth {
            Ok(device_id) => session(state, socket, device_id).await,
            Err(code) => close_immediately(socket, code).await,
        }
    })
}

/// Header 里取 Beare、
fn bearer(headers: &HeaderMap) -> Option<String> {
    let v = headers.get("authorization")?.to_str().ok()?;
    let t = v.strip_prefix("Bearer ")?;
    Some(t.to_string())
}

async fn close_immediately(socket: WebSocket, code: u16) {
    let (mut sender, _) = socket.split();
    let _ = sender
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code,
            reason: std::borrow::Cow::from(""),
        })))
        .await;
}

async fn session(state: Arc<Inner>, socket: WebSocket, device_id: Uuid) {
    let (mut sender, mut receiver) = socket.split();
    let (tx_out, mut rx_out) = mpsc::channel(32);

    // 同设备双连接：旧通道收到 Close(4409) 自行退出，新通道顶替
    if let Some(old) = state.rescue_hub.insert(device_id, tx_out.clone()) {
        let _ = old.send(Outbound::Close(4409)).await;
    }

    let ping_interval = state.cfg.ping_interval;
    let pump = tokio::spawn(async move {
        let mut ping = tokio::time::interval(ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                out = rx_out.recv() => match out {
                    Some(Outbound::Frame(env)) => {
                        if sender
                            .send(Message::Text(serde_json::to_string(&env).unwrap_or_default()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Some(Outbound::Close(code)) => {
                        let _ = sender
                            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                code,
                                reason: std::borrow::Cow::from(""),
                            })))
                            .await;
                        break;
                    }
                    None => break,
                },
                _ = ping.tick() => {

                    if sender.send(Message::Ping(vec![])).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    tracing::info!(device_id = %device_id, "rescue ws session start");
    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(device_id = %device_id, "rescue ws read error: {e}");
                break;
            }
        };
        match msg {
            Message::Text(text) => handle_frame(&state, &device_id, &text).await,

            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(c) => {
                tracing::info!(device_id = %device_id, ?c, "rescue ws closed by peer");
                break;
            }
        }
    }

    tracing::info!(device_id = %device_id, "rescue ws session end");
    pump.abort();
    // remove_mine = false 表示自己已被顶替：离线收口归新通道管，不在此触发
    if state.rescue_hub.remove_mine(device_id, &tx_out) {
        state.on_device_offline(device_id).await;
    }
}

async fn handle_frame(state: &Arc<Inner>, device_id: &Uuid, text: &str) {
    let env: Envelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(device_id = %device_id, "rescue frame parse failed: {e}");
            return;
        }
    };
    match env.typ.as_str() {
        "pty.data" | "pty.exit" => pty_route(state, &env).await,
        other => {
            tracing::warn!(device_id = %device_id, typ = other, "rescue 通道只承载 pty.*")
        }
    }
}

async fn pty_route(state: &Arc<Inner>, env: &Envelope) {
    let Some(sid) = env.data["session_id"]
        .as_str()
        .and_then(|s| s.parse::<Uuid>().ok())
    else {
        return;
    };
    let Some(ch) = state.pty_sessions.get(&sid) else {
        return;
    };
    let tx = ch.tx.clone();
    drop(ch);

    match env.typ.as_str() {
        "pty.data" => {
            use base64::Engine;
            if let Some(b) = env.data["data"]
                .as_str()
                .and_then(|d| base64::engine::general_purpose::STANDARD.decode(d).ok())
            {
                let _ = tx.send(PtyEvent::Data(b)).await;
            }
        }
        "pty.exit" => {
            let code = env.data["code"].as_i64().unwrap_or(0) as i32;
            let _ = tx.send(PtyEvent::Exit(code)).await;
        }
        _ => {}
    }
}
