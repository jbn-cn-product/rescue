//! PTY 会话引擎

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Mutex;
use tokio::sync::mpsc;

pub fn envelope(typ: &str, data: serde_json::Value) -> String {
    serde_json::json!({
        "v": 1,
        "id": uuid::Uuid::new_v4().to_string(),
        "type": typ,
        "ts": now_ms(),
        "data": data,
    })
    .to_string()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

struct Session {
    master: Box<dyn MasterPty + Send>,
    writer: Box<dyn Write + Send>,
}

/// session_id
static SESSIONS: Mutex<Option<HashMap<String, Session>>> = Mutex::new(None);

fn with_sessions<R>(f: impl FnOnce(&mut HashMap<String, Session>) -> R) -> Option<R> {
    let mut g = SESSIONS.lock().ok()?;
    let map = g.get_or_insert_with(HashMap::new);
    Some(f(map))
}

pub fn open(data: &serde_json::Value, out: &mpsc::Sender<String>) {
    let Some(sid) = data["session_id"].as_str().filter(|s| !s.is_empty()) else {
        tracing::warn!("pty.open 缺 session_id，忽略");
        return;
    };
    let rows = data["rows"].as_u64().unwrap_or(24).clamp(2, 200) as u16;
    let cols = data["cols"].as_u64().unwrap_or(80).clamp(10, 500) as u16;

    let pty = match native_pty_system().openpty(PtySize {
        rows,
        cols,
        ..Default::default()
    }) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("pty.open 失败（openpty）: {e}");
            return;
        }
    };

    let mut cmd = CommandBuilder::new_default_prog();
    cmd.env("TERM", "xterm-256color");
    let mut child = match pty.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("pty.open 失败（spawn shell）: {e}");
            return;
        }
    };

    let mut reader = match pty.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("pty.open 失败（clone reader）: {e}");
            let _ = child.kill();
            return;
        }
    };
    let writer = match pty.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("pty.open 失败（take writer）: {e}");
            let _ = child.kill();
            return;
        }
    };
    // slave 立刻 drop（见模块注释）；master 的原 fd 由 Session 持有
    drop(pty.slave);

    // 单会话防重（平台保证单设备 1 会话；这里只是防御）：同 id 重放忽略，异 id 拒绝
    let inserted = with_sessions(|m| {
        if m.contains_key(sid) {
            false
        } else if !m.is_empty() {
            tracing::warn!(have = ?m.keys().collect::<Vec<_>>(), "pty.open 被拒：已有活动会话");
            false
        } else {
            m.insert(
                sid.to_string(),
                Session {
                    master: pty.master,
                    writer,
                },
            );
            true
        }
    });
    if !inserted.unwrap_or(false) {
        let _ = child.kill();
        return;
    }
    tracing::info!(session_id = sid, rows, cols, "pty 会话已建立");

    let out = out.clone();
    let sid = sid.to_string();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let frame = envelope(
                        "pty.data",
                        serde_json::json!({
                            "session_id": sid,
                            "data": B64.encode(&buf[..n]),
                        }),
                    );
                    if out.blocking_send(frame).is_err() {
                        break;
                    }
                }
            }
        }

        let code = child.wait().ok().map(|s| s.exit_code()).unwrap_or(0) as i32;

        let still_there = with_sessions(|m| m.remove(&sid).is_some()).unwrap_or(false);
        if still_there {
            let _ = out.blocking_send(envelope(
                "pty.exit",
                serde_json::json!({ "session_id": sid, "code": code }),
            ));
            tracing::info!(session_id = sid, code, "pty 会话结束");
        }
    });
}

/// 写输入
pub fn input(data: &serde_json::Value) {
    let Some(sid) = data["session_id"].as_str() else {
        return;
    };
    let Ok(bytes) = B64.decode(data["data"].as_str().unwrap_or_default()) else {
        return;
    };
    with_sessions(|m| {
        if let Some(s) = m.get_mut(sid) {
            // 写失败不摘会话：读线程会在 EOF 时统一收口
            if let Err(e) = s.writer.write_all(&bytes).and_then(|_| s.writer.flush()) {
                tracing::warn!("pty.input 写失败: {e}");
            }
        }
    });
}

pub fn resize(data: &serde_json::Value) {
    let Some(sid) = data["session_id"].as_str() else {
        return;
    };
    let rows = data["rows"].as_u64().unwrap_or(24).clamp(2, 200) as u16;
    let cols = data["cols"].as_u64().unwrap_or(80).clamp(10, 500) as u16;
    with_sessions(|m| {
        if let Some(s) = m.get(sid) {
            if let Err(e) = s.master.resize(PtySize {
                rows,
                cols,
                ..Default::default()
            }) {
                tracing::warn!("pty.resize 失败: {e}");
            }
        }
    });
}

/// 单会话关闭：摘表 + drop（master fd 全关 → SIGHUP → shell 退出）。
pub fn close(data: &serde_json::Value) {
    let Some(sid) = data["session_id"].as_str() else {
        return;
    };
    let removed = with_sessions(|m| m.remove(sid).is_some()).unwrap_or(false);
    if removed {
        tracing::info!(session_id = sid, "pty 会话被远端关闭");
    }
}

/// WS 断线：全部会话立即摘除（不留孤儿 root shell）。
pub fn kill_all() {
    let n = with_sessions(|m| {
        let n = m.len();
        m.clear();
        n
    })
    .unwrap_or(0);
    if n > 0 {
        tracing::warn!(count = n, "WS 断开，已关闭全部 pty 会话");
    }
}
