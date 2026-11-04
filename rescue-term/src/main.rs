//! rescue-term：独立 PTY 终端代理
//! 部署）：二进制 + 独立 systemd unit（root，
//! Restart=always）。尽量不更新

mod pty;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const BACKOFF_INIT: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(30);
/// 配置不可用时的重读周期（这是"yaml 坏了不退出"的节拍，不是重连节拍）
const CFG_RETRY: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Cfg {
    endpoint: String,
    sn: String,
    token: String,
}

#[derive(Deserialize)]
struct FileCfg {
    #[serde(default)]
    rescue: Option<PlatformSec>,
    #[serde(default)]
    platform: Option<PlatformSec>,
}

#[derive(Deserialize)]
struct PlatformSec {
    #[serde(default = "yes")]
    enabled: bool,
    endpoint: String,
    sn: String,
    token: String,
}

fn yes() -> bool {
    true
}

fn read_config(path: &str) -> anyhow::Result<Option<PlatformSec>> {
    let text = std::fs::read_to_string(path)?;
    let cfg: FileCfg = serde_yaml::from_str(&text)?;
    Ok(cfg.rescue.or(cfg.platform).filter(|p| p.enabled))
}

fn env_or(name: &str, slot: &mut String) {
    if let Ok(v) = std::env::var(name) {
        if !v.trim().is_empty() {
            *slot = v.trim().to_string();
        }
    }
}

fn apply_env(mut c: PlatformSec) -> Cfg {
    env_or("RESCUE_ENDPOINT", &mut c.endpoint);
    env_or("RESCUE_TOKEN", &mut c.token);
    env_or("RESCUE_SN", &mut c.sn);
    env_or("EDGE_PLATFORM_ENDPOINT", &mut c.endpoint);
    env_or("EDGE_PLATFORM_TOKEN", &mut c.token);
    env_or("EDGE_PLATFORM_SN", &mut c.sn);
    Cfg {
        endpoint: c.endpoint,
        sn: c.sn,
        token: c.token,
    }
}

/// CLI 覆盖
#[derive(Clone, Default)]
struct Cli {
    endpoint: Option<String>,
    sn: Option<String>,
    token: Option<String>,
}

fn apply_cli(c: &mut Cfg, cli: &Cli) {
    if let Some(v) = &cli.endpoint {
        c.endpoint = v.clone();
    }
    if let Some(v) = &cli.sn {
        c.sn = v.clone();
    }
    if let Some(v) = &cli.token {
        c.token = v.clone();
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr) // 只走 stderr → journald：不依赖 logging 配置段
        .init();

    let mut args = std::env::args().skip(1);
    let mut cfg_path = "rescue-term.yaml".to_string();
    let mut cli = Cli::default();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" | "-c" => cfg_path = args.next().unwrap_or(cfg_path),
            "--endpoint" => cli.endpoint = Some(args.next().unwrap_or_default()),
            "--sn" => cli.sn = Some(args.next().unwrap_or_default()),
            "--token" => cli.token = Some(args.next().unwrap_or_default()),
            other => {
                eprintln!(
                    "未知参数: {other}\n用法: rescue-term [--config <yaml>] \
                     [--endpoint <url>] [--sn <sn>] [--token <bearer>]"
                );
                std::process::exit(2);
            }
        }
    }

    let mut backoff = BACKOFF_INIT;
    let mut last_cfg: Option<Cfg> = None;
    loop {
        match read_config(&cfg_path) {
            Ok(Some(c)) => {
                let mut c = apply_env(c);
                apply_cli(&mut c, &cli);
                tracing::info!(endpoint = %c.endpoint, sn = %c.sn, "配置已加载");
                last_cfg = Some(c);
            }
            Ok(None) => {
                tracing::warn!("rescue/platform 段缺失或未启用，等待配置…");
                last_cfg = None;
            }
            Err(e) => {
                if last_cfg.is_some() {
                    tracing::warn!("配置读取失败，沿用内存中的上一份配置: {e:#}");
                } else {
                    tracing::error!(
                        "配置读取失败且无可用副本（{}s 后重试）: {e:#}",
                        CFG_RETRY.as_secs()
                    );
                }
            }
        }

        let Some(cfg) = last_cfg.clone() else {
            tokio::time::sleep(CFG_RETRY).await;
            continue;
        };

        match connect(&cfg).await {
            Ok(ws) => {
                backoff = BACKOFF_INIT;
                session(&cfg, ws).await;
            }
            Err(e) => tracing::warn!("连接平台失败（{}s 后重试）: {e}", backoff.as_secs()),
        }
        // 断线即关全部终端会话
        pty::kill_all();
        tokio::time::sleep(backoff.mul_f64(jitter_factor())).await;
        backoff = (backoff * 2).min(BACKOFF_CAP);
    }
}

fn jitter_factor() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64
        / 1e9;
    0.8 + 0.4 * nanos
}

/// Bearer 在握手 Header 里
async fn connect(cfg: &Cfg) -> anyhow::Result<WsStream> {
    let base = cfg
        .endpoint
        .replace("https://", "wss://")
        .replace("http://", "ws://");
    let url = format!("{base}/ws/rescue");
    let mut req = url.into_client_request()?;
    req.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", cfg.token)
            .parse()
            .map_err(|_| anyhow::anyhow!("token 含非法 Header 字符"))?,
    );
    let (ws, _resp) = tokio_tungstenite::connect_async(req).await?;
    Ok(ws)
}

async fn session(cfg: &Cfg, ws: WsStream) {
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = mpsc::channel::<String>(64);
    tracing::info!(sn = %cfg.sn, "救援通道已连接");

    loop {
        tokio::select! {
            frame = rx.recv() => match frame {
                Some(f) => {
                    if sink.send(Message::Text(f.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            msg = stream.next() => match msg {
                Some(Ok(Message::Text(t))) => handle_frame(&tx, t.as_str()),
                // 协议层保活：平台 pump 30s 发 Ping，tungstenite 读到后自动补 Pong
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(Some(f)))) => {
                    // 4401/4403 在 edge 侧是终态；救援保持慢重试：
                    // token 被重置后运维会改配置重启，救援修好自己就该回来
                    let code = u16::from(f.code);
                    tracing::error!(code, "救援通道被平台关闭（4401=token 失效，4403=设备已退役）");
                    break;
                }
                Some(Ok(Message::Close(None))) | Some(Err(_)) | None => break,
            },
        }
    }
    tracing::warn!("救援通道断开");
}

fn handle_frame(tx: &mpsc::Sender<String>, text: &str) {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("平台帧不是合法 JSON: {e}");
            return;
        }
    };
    if v["v"].as_u64() != Some(1) {
        tracing::warn!(v = ?v["v"], "未知信封版本，丢弃");
        return;
    }
    match v["type"].as_str().unwrap_or("") {
        "pty.open" => pty::open(&v["data"], tx),
        "pty.input" => pty::input(&v["data"]),
        "pty.resize" => pty::resize(&v["data"]),
        "pty.close" => pty::close(&v["data"]),
        other => tracing::debug!(typ = other, "忽略平台帧"),
    }
}
