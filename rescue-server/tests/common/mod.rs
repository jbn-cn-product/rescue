//! 集成测试基建：Mock Hooks + 假设备（扮 rescue-term）+ 假浏览器 + TestServer。

use futures_util::{SinkExt, StreamExt};
use rescue_server::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

pub const USER_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001);

// ---------- Mock Hooks ----------

#[derive(Debug, Clone, PartialEq)]
pub struct EndRec {
    pub session_id: Uuid,
    pub reason: EndReason,
    pub recording: RecordingOutcome,
}

#[derive(Clone)]
pub struct MockHooks {
    pub device_id: Uuid,
    starts: Arc<Mutex<Vec<Uuid>>>,
    ends: Arc<Mutex<Vec<EndRec>>>,
    audits: Arc<Mutex<Vec<String>>>,
    recording: Arc<Mutex<RecordingOutcome>>,
}

impl MockHooks {
    fn new(device_id: Uuid) -> Self {
        Self {
            device_id,
            starts: Default::default(),
            ends: Default::default(),
            audits: Default::default(),
            recording: Arc::new(Mutex::new(RecordingOutcome::Saved {
                key: "test-key".into(),
                truncated: false,
            })),
        }
    }

    pub fn set_recording(&self, o: RecordingOutcome) {
        *self.recording.lock().unwrap() = o;
    }

    pub fn ends(&self) -> Vec<EndRec> {
        self.ends.lock().unwrap().clone()
    }

    pub fn audits(&self) -> Vec<String> {
        self.audits.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Hooks for MockHooks {
    async fn auth_device(&self, token: &str) -> Result<Uuid, u16> {
        match token {
            "good-token" => Ok(self.device_id),
            "retired-token" => Err(4403),
            _ => Err(4401),
        }
    }

    async fn auth_browser(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Uuid, axum::http::StatusCode> {
        match headers.get("x-test-user").and_then(|v| v.to_str().ok()) {
            Some("alice") => Ok(USER_ID),
            _ => Err(axum::http::StatusCode::UNAUTHORIZED),
        }
    }

    async fn session_started(&self, s: &SessionStart) {
        self.starts.lock().unwrap().push(s.session_id);
    }

    async fn session_ended(&self, e: &SessionEnd) {
        self.ends.lock().unwrap().push(EndRec {
            session_id: e.session_id,
            reason: e.reason,
            recording: e.recording.clone(),
        });
    }

    async fn save_recording(&self, _id: Uuid, bytes: &[u8], truncated: bool) -> RecordingOutcome {
        // 产品语义：把 crate 传入的真实字节数与自己的结局意图合成三态
        match self.recording.lock().unwrap().clone() {
            RecordingOutcome::Saved { key, .. } => RecordingOutcome::Saved { key, truncated },
            RecordingOutcome::Lost { .. } => RecordingOutcome::Lost {
                bytes: bytes.len(),
                truncated,
            },
            RecordingOutcome::NotRecorded => RecordingOutcome::NotRecorded,
        }
    }

    async fn audit(&self, e: &AuditEvent) {
        self.audits.lock().unwrap().push(e.action.to_string());
    }
}

// ---------- TestServer ----------

pub struct TestServer {
    pub rescue: Arc<RescueServer>,
    pub hooks: MockHooks,
    port: u16,
    device: Uuid,
}

impl TestServer {
    pub async fn start(cfg: Config) -> Self {
        let device = Uuid::new_v4();
        let hooks = MockHooks::new(device);
        let rescue = Arc::new(RescueServer::new(Arc::new(hooks.clone()), cfg));
        let app = rescue.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            rescue,
            hooks,
            port,
            device,
        }
    }

    pub fn device_id(&self) -> Uuid {
        self.device
    }

    /// ws://：客户端握手 URL（失败时仍以 Http 错误携带状态码返回）
    fn base(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// 等设备通道注册进 rescue_hub（issue_ticket 的前置）
    pub async fn wait_device_online(&self) {
        for _ in 0..200 {
            if self.rescue.rescue_online(self.device) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("设备通道未在 2s 内注册");
    }

    pub async fn issue_ticket(&self) -> String {
        self.wait_device_online().await;
        let (t, _ttl) = self.rescue.issue_ticket(USER_ID, self.device).unwrap();
        t.to_string()
    }

    pub async fn connect_device(&self) -> FakeDevice {
        self.connect_device_with("good-token").await
    }

    pub async fn connect_device_with(&self, token: &str) -> FakeDevice {
        let req = format!("{}/ws/rescue", self.base())
            .into_client_request()
            .unwrap();
        let mut req = req;
        req.headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        FakeDevice::run(ws)
    }

    /// 浏览器握手失败 → 返回 HTTP 状态码
    pub async fn try_browser(&self, ticket: String, device: &Uuid) -> u16 {
        let url = format!("{}/ws/pty/{}?ticket={}", self.base(), device, ticket);
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("x-test-user", "alice".parse().unwrap());
        match tokio_tungstenite::connect_async(req).await {
            Ok(_) => panic!("预期握手失败"),
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => r.status().as_u16(),
            Err(e) => panic!("预期 HTTP 拒绝，得到: {e}"),
        }
    }

    pub async fn connect_browser(&self, ticket: String) -> FakeBrowser {
        let url = format!("{}/ws/pty/{}?ticket={}", self.base(), self.device, ticket);
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("x-test-user", "alice".parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        FakeBrowser {
            ws,
            buf: Vec::new(),
        }
    }
}

// ---------- 假设备：默认行为 = 开壳应答 + 输入回显 + exit 退出 ----------

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

enum Cmd {
    Disconnect,
}

enum Evt {
    Open(String),
    PtyClose,
    WsClosed(u16),
}

pub struct FakeDevice {
    cmd: mpsc::Sender<Cmd>,
    evt: mpsc::Receiver<Evt>,
}

impl FakeDevice {
    fn run(ws: Ws) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(4);
        let (evt_tx, evt_rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let (mut sink, mut stream) = ws.split();
            let mut sid = String::new();
            loop {
                tokio::select! {
                    c = cmd_rx.recv() => match c {
                        Some(Cmd::Disconnect) | None => {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                    },
                    msg = stream.next() => match msg {
                        Some(Ok(Message::Text(t))) => {
                            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                            match v["type"].as_str().unwrap_or("") {
                                "pty.open" => {
                                    sid = v["data"]["session_id"].as_str().unwrap().to_string();
                                    let _ = evt_tx.send(Evt::Open(sid.clone())).await;
                                }
                                "pty.input" => {
                                    use base64::Engine;
                                    let b = base64::engine::general_purpose::STANDARD
                                        .decode(v["data"]["data"].as_str().unwrap_or(""))
                                        .unwrap_or_default();
                                    // exit\n → 模拟 shell 退出；其余原样回显
                                    let frame = if b == b"exit\n" {
                                        serde_json::json!({
                                            "v": 1, "id": Uuid::new_v4().to_string(),
                                            "type": "pty.exit", "ts": 0,
                                            "data": { "session_id": sid, "code": 0 },
                                        })
                                    } else {
                                        serde_json::json!({
                                            "v": 1, "id": Uuid::new_v4().to_string(),
                                            "type": "pty.data", "ts": 0,
                                            "data": {
                                                "session_id": sid,
                                                "data": base64::engine::general_purpose::STANDARD.encode(&b),
                                            },
                                        })
                                    };
                                    if sink.send(Message::Text(frame.to_string())).await.is_err() {
                                        break;
                                    }
                                }
                                "pty.close" => { let _ = evt_tx.send(Evt::PtyClose).await; }
                                _ => {}
                            }
                        }
                        Some(Ok(Message::Close(Some(f)))) => {
                            let _ = evt_tx.send(Evt::WsClosed(f.code.into())).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => {
                            let _ = evt_tx.send(Evt::WsClosed(0)).await;
                            break;
                        }
                    },
                }
            }
        });
        Self {
            cmd: cmd_tx,
            evt: evt_rx,
        }
    }

    async fn next_evt(&mut self) -> Evt {
        tokio::time::timeout(Duration::from_secs(5), self.evt.recv())
            .await
            .expect("5s 内应有设备侧事件")
            .expect("设备任务未死")
    }

    /// 等 pty.open（浏览器开壳触发），返回 session_id
    pub async fn expect_open(&mut self) -> String {
        match self.next_evt().await {
            Evt::Open(s) => s,
            _ => panic!("预期 pty.open"),
        }
    }

    /// 等 pty.close
    pub async fn expect_frame(&mut self, typ: &str) {
        assert_eq!(typ, "pty.close", "假设备只跟踪 pty.close");
        match self.next_evt().await {
            Evt::PtyClose => {}
            _ => panic!("预期 pty.close"),
        }
    }

    pub async fn wait_close_code(&mut self) -> u16 {
        match self.next_evt().await {
            Evt::WsClosed(c) => c,
            _ => panic!("预期通道 Close"),
        }
    }

    pub async fn disconnect(&self) {
        let _ = self.cmd.send(Cmd::Disconnect).await;
    }
}

// ---------- 假浏览器 ----------

pub struct FakeBrowser {
    ws: Ws,
    buf: Vec<u8>,
}

impl FakeBrowser {
    pub async fn send_text(&mut self, v: serde_json::Value) {
        use futures_util::SinkExt;
        self.ws.send(Message::Text(v.to_string())).await.unwrap();
    }

    pub async fn send_resize(&mut self, rows: u64, cols: u64) {
        self.send_text(serde_json::json!({ "type": "resize", "rows": rows, "cols": cols }))
            .await;
    }

    pub async fn send_input(&mut self, bytes: &[u8]) {
        use base64::Engine;
        let d = base64::engine::general_purpose::STANDARD.encode(bytes);
        self.send_text(serde_json::json!({ "type": "input", "data": d }))
            .await;
    }

    pub async fn send_close(&mut self) {
        self.send_text(serde_json::json!({ "type": "close" })).await;
    }

    /// 读到包含 marker 的 data 帧为止（逐帧超时 5s）；到 closed/exit 先返回
    async fn read_one(&mut self) -> serde_json::Value {
        use futures_util::StreamExt;
        let msg = tokio::time::timeout(Duration::from_secs(5), self.ws.next())
            .await
            .expect("5s 内应有帧")
            .unwrap()
            .expect("浏览器通道未断");
        match msg {
            Message::Text(t) => serde_json::from_str(&t).unwrap(),
            Message::Close(_) => serde_json::json!({ "type": "closed", "reason": "ws_closed" }),
            _ => serde_json::json!({ "type": "other" }),
        }
    }

    pub async fn wait_for(&mut self, marker: &[u8]) -> Option<()> {
        loop {
            let m = self.read_one().await;
            if m["type"] == "data" {
                use base64::Engine;
                let b = base64::engine::general_purpose::STANDARD
                    .decode(m["data"].as_str().unwrap())
                    .unwrap_or_default();
                self.buf.extend_from_slice(&b);
                if self
                    .buf
                    .iter()
                    .as_slice()
                    .windows(marker.len())
                    .any(|w| w == marker)
                {
                    self.buf.clear();
                    return Some(());
                }
            } else if m["type"] == "closed" {
                return None;
            }
        }
    }

    pub async fn wait_closed(&mut self) -> serde_json::Value {
        loop {
            let m = self.read_one().await;
            if m["type"] == "closed" {
                return m;
            }
        }
    }
}
