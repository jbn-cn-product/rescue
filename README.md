
# Rescue

设备救援终端协议的 Rust 实现：为无 OTA、需人工介入的边缘设备提供一条**受控的远程 shell 通道**。

服务端不持有任何产品概念（设备表、用户体系、存储位置），鉴权 / 落库 / 录制 / 审计全部通过 `Hooks` 由产品侧注入，协议行为独立于宿主业务流程。

## 组成

| crate | 版本 | 角色 | 部署位置 |
| --- | --- | --- | --- |
| `rescue-server` | 0.1.0 | 协议服务端**库**（不是独立服务） | 产品服务器，由宿主进程把 `RescueServer::router()` 挂到自己的 axum Router 上 |
| `rescue-term` | 0.2.0 | 独立 PTY 终端代理 | 设备侧，root 运行的 systemd 常驻进程 |

## 链路

```
浏览器(xterm.js)          产品服务器(rescue-server)              设备(rescue-term)
     │                              │                                  │
     │                              │◄───── ① WS /ws/rescue(Bearer) ────┤  设备长连接，先建立
     │                              │                                  │
     │── ② 申请 ticket ────────────►│                                  │
     │◄─ 200 {ticket, ttl} ─────────│                                  │
     │                              │                                  │
     │── ③ WS /ws/pty/:id?ticket=──►│                                  │
     │                              │──── ④ pty.open(session_id) ─────►│  创建 PTY 与 shell
     │                              │                                  │
     │── ⑤ input/resize ───────────►│──── pty.input / pty.resize ─────►│
     │◄───────────── data ──────────│◄─── pty.data(base64) ────────────┤
     │◄───────────── exit ──────────│◄─── pty.exit(code) ──────────────┤
```

浏览器侧走 **Cookie JWT + 一次性 ticket** 双因子；设备侧走 **Bearer token**（放在握手 Header，不放 query，避免 token 写进任何代理日志）。

## 协议

统一信封，`v` 固定为 `1`，版本不符直接丢弃：

```json
{ "v": 1, "id": "<uuid>", "type": "pty.data", "ts": 1762000000000, "data": {} }
```

服务端 ↔ 设备通道（`/ws/rescue`）只承载 `pty.*`：

| type | 方向 | data |
| --- | --- | --- |
| `pty.open` | server → device | `session_id`, `rows`, `cols` |
| `pty.input` | server → device | `session_id`, `data`(base64) |
| `pty.resize` | server → device | `session_id`, `rows`, `cols` |
| `pty.close` | server → device | `session_id` |
| `pty.data` | device → server | `session_id`, `data`(base64) |
| `pty.exit` | device → server | `session_id`, `code` |

浏览器 ↔ 服务端通道（`/ws/pty/:device_id`）为不带信封的裸 JSON：

| 浏览器 → 服务端 | 说明 |
| --- | --- |
| `{"type":"input","data":"<base64>"}` | 键盘输入 |
| `{"type":"resize","rows":N,"cols":N}` | 终端尺寸，首帧 resize 决定 PTY 初始尺寸 |
| `{"type":"close"}` | 用户主动关闭 |

| 服务端 → 浏览器 | 说明 |
| --- | --- |
| `{"type":"data","data":"<base64>"}` | 输出字节 |
| `{"type":"exit","code":N}` | shell 退出 |
| `{"type":"closed","reason":"..."}` | 会话收口，随后发 Close 帧 |

尺寸取值域：`rows` 钳制到 2–200，`cols` 钳制到 10–500。首帧 resize 最多等 5s，超时按 24×80 开。

## 快速开始（服务端）

```rust
use std::sync::Arc;
use rescue_server::{Config, Hooks, RescueServer};

// 1. 用产品自己的鉴权/落库实现 Hooks
let server = RescueServer::new(Arc::new(MyHooks), Config::default());

// 2. 挂到现有 axum 服务上
let app = axum::Router::new()
    .nest("/rescue", server.router())
    .with_state(my_app_state);

// 3. 设备在线时下发一次性票据，前端拿到票据再连 /ws/pty/:device_id
match server.issue_ticket(user_id, device_id) {
    Ok((ticket, ttl_secs)) => { /* 返回给前端 */ }
    Err(TicketError::DeviceOffline) => { /* 设备终端通道不在线 */ }
}
```

`RescueServer` 对外还提供：

| 方法 | 用途 |
| --- | --- |
| `router()` | 返回挂载两条 WS 通道的 `axum::Router` |
| `issue_ticket(user_id, device_id)` | 下发一次性票据，设备离线时返回 `TicketError::DeviceOffline` |
| `rescue_online(device_id)` | 设备救援通道是否在线 |
| `open_sessions(device_id)` | 该设备当前打开的会话数 |

### Config 默认值

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `ticket_ttl` | 60s | 票据有效期，一次性消费 |
| `idle_timeout` | 600s | 无输入输出即挂断 |
| `record_max_bytes` | 8 MiB | 单会话录制上限，超出截断并标记 `truncated` |
| `ping_interval` | 30s | 协议层 Ping，维持 NAT 空闲映射 |

### Hooks

```rust
#[async_trait::async_trait]
pub trait Hooks: Send + Sync + 'static {
    async fn auth_device(&self, token: &str) -> Result<Uuid, u16>;
    async fn auth_browser(&self, headers: &HeaderMap) -> Result<Uuid, StatusCode>;
    async fn session_started(&self, s: &SessionStart) {}
    async fn session_ended(&self, s: &SessionEnd) {}
    async fn save_recording(&self, session_id: Uuid, bytes: &[u8], truncated: bool) -> RecordingOutcome;
    async fn audit(&self, e: &AuditEvent) {}
}
```

两个鉴权 Hook 的失败语义不同，这是刻意的：

- `auth_device` 在 WS upgrade **之后**执行 → 返回 close code（如 `4401`）；
- `auth_browser` 在 upgrade **之前**执行 → 返回 HTTP 状态码。

`session_ended` 携带 `EndReason`：`user_close` / `browser_closed` / `device_offline` / `idle_timeout` / `shell_exit` / `internal`。
`save_recording` 的返回 `RecordingOutcome` 会原样回传：`NotRecorded` / `Saved { key, truncated }` / `Lost { bytes, truncated }`——落库失败不会被悄悄吞掉。

### 关闭码与错误

| 码 | 场景 |
| --- | --- |
| `4401` | 设备 token 无效（设备侧视为终态，救援链路保持慢重试，等运维改配置） |
| `4403` | 设备已退役 |
| `4409` | 同设备新连接顶替旧连接 |

| HTTP | code | 场景 |
| --- | --- | --- |
| `423` | `ticket_invalid` | 票据无效 / 过期 / 设备或用户不匹配 |
| `409` | `device_offline` | 设备终端通道不在线 |
| `409` | `session_exists` | 该设备已有终端会话（单设备单会话） |

## 快速开始（设备端）

```yaml
# /opt/rescue-term/config/rescue-term.yaml
rescue:              # 旧字段名 platform 同样兼容
  enabled: true
  endpoint: https://your-host
  sn: DEVICE-SN
  token: <设备令牌>
```

配置优先级：**CLI 参数 > 环境变量 > YAML 文件**。

| 来源 | 字段 |
| --- | --- |
| CLI | `--config/-c <yaml>`、`--endpoint <url>`、`--sn <sn>`、`--token <bearer>` |
| 环境变量 | `RESCUE_ENDPOINT` / `RESCUE_TOKEN` / `RESCUE_SN`，兼容 `EDGE_PLATFORM_*` |

运行时行为：

- **配置坏了不自杀**：读取失败时沿用内存中的上一份配置；无可用副本则每 10s 重读，进程不退出。
- **重连退避**：1s 起指数翻倍，30s 封顶，附带 0.8–1.2 抖动，避免整批设备同时重连。
- **断线即清场**：WS 断开立刻关闭全部 PTY 会话，不留孤儿 root shell。
- **单会话防重**：同 `session_id` 重放忽略，已有其它活动会话时拒绝新建（服务端本身已保证单设备单会话，这里是防御性兜底）。
- **异常输入即丢**：非法 base64 的 `pty.input` 直接丢弃——往 shell 写半截解码比不写更糟。

### 部署

从 [Releases](../../releases) 下载对应架构的 `rescue-term-<版本>-<target>.tar.gz`（附 sha256 校验和），解压后在包目录内执行：

```bash
tar -xzf rescue-term-<版本>-<target>.tar.gz
cd rescue-term-<版本>-<target>
./install.sh          # 首次安装会写入示例配置并提示填写
vi /opt/rescue-term/config/rescue-term.yaml   # 填入 endpoint / sn / token
systemctl start rescue-term
journalctl -u rescue-term -f
```

`install.sh` 等价于手工安装：

```bash
install -D rescue-term /opt/rescue-term/rescue-term
install -D rescue-term.service /etc/systemd/system/rescue-term.service
systemctl daemon-reload && systemctl enable --now rescue-term
journalctl -u rescue-term -f
```

unit 以 **root** 运行（终端里需要对宿主服务做 `systemctl` 操作），`Restart=always`，日志只走 stderr → journald，不依赖 logging 配置段。




