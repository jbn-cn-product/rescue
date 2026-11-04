//! 假设备（tokio-tungstenite 扮 rescue-term）
//! + 假浏览器 + Mock Hooks。每个用例锚一条规范条款。

mod common;

use common::*;
use rescue_server::*;
use uuid::Uuid;

#[tokio::test]
async fn full_round_trip_and_user_close() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;

    br.send_resize(24, 120).await;
    let sid = dev.expect_open().await;
    br.send_input(b"echo RT_OK\n").await;
    let out = br.wait_for(b"RT_OK").await;
    assert!(out.is_some(), "浏览器应收到设备回显");

    br.send_close().await;
    let last = br.wait_closed().await;
    assert_eq!(last["reason"], "user_close");

    dev.expect_frame("pty.close").await;

    let ends = hooks.ends();
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0].session_id.to_string(), sid);
    assert_eq!(ends[0].reason, EndReason::UserClose);
    assert_eq!(
        ends[0].recording,
        RecordingOutcome::Saved {
            key: "test-key".into(),
            truncated: false
        }
    );
    assert_eq!(hooks.audits(), vec!["pty.open", "pty.close"]);
}

#[tokio::test]
async fn ticket_one_time_and_device_binding() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();

    let _dev = srv.connect_device().await;
    srv.wait_device_online().await; // 票据前置：通道注册

    let ticket = srv.issue_ticket().await;

    let status = srv.try_browser(ticket.clone(), &Uuid::new_v4()).await;
    assert_eq!(status, 423, "四元组不匹配必须拒绝");

    let mut br = srv.connect_browser(ticket.to_string()).await;
    br.send_resize(24, 80).await;
    br.send_input(b"t2\n").await;
    br.wait_for(b"t2").await;
    br.send_close().await;
    br.wait_closed().await;

    let status = srv.try_browser(ticket, &srv.device_id()).await;
    assert_eq!(status, 423);
    assert!(!hooks.ends().is_empty());
}

#[tokio::test]
async fn device_replacement_4409() {
    let srv = TestServer::start(Config::default()).await;
    let mut first = srv.connect_device().await;
    let _second = srv.connect_device().await;
    let code = first.wait_close_code().await;
    assert_eq!(code, 4409, "旧连接应被 Close(4409) 顶替");
    drop(_second);
}

#[tokio::test]
async fn bad_device_token_close_4401() {
    let srv = TestServer::start(Config::default()).await;
    let mut dev = srv.connect_device_with("bad-token").await;
    let code = dev.wait_close_code().await;
    assert_eq!(code, 4401);
}

#[tokio::test]
async fn device_disconnect_closes_sessions() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;
    br.send_resize(24, 80).await;
    dev.expect_open().await;

    dev.disconnect().await; // 设备通道断（不经 pty.close）
    let last = br.wait_closed().await;
    assert_eq!(last["reason"], "device_offline");
    let ends = hooks.ends();
    assert_eq!(ends[0].reason, EndReason::DeviceOffline);
}

#[tokio::test]
async fn recording_lost_is_propagated() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();
    hooks.set_recording(RecordingOutcome::Lost {
        bytes: 42,
        truncated: true,
    });

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;
    br.send_resize(24, 80).await;
    dev.expect_open().await;
    br.send_input(b"t6\n").await;
    br.wait_for(b"t6").await;
    br.send_close().await;
    br.wait_closed().await;

    let ends = hooks.ends();
    assert_eq!(
        ends[0].recording,
        RecordingOutcome::Lost {
            bytes: 6,
            truncated: false
        },
        "Lost 必须带真实字节数透传给产品"
    );
}

#[tokio::test]
async fn idle_timeout_closes_session() {
    let cfg = Config {
        idle_timeout: std::time::Duration::from_millis(300),
        ..Default::default()
    };
    let srv = TestServer::start(cfg).await;
    let hooks = srv.hooks.clone();

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;
    br.send_resize(24, 80).await;
    dev.expect_open().await;

    let last = br.wait_closed().await;
    assert_eq!(last["reason"], "idle_timeout");
    assert_eq!(hooks.ends()[0].reason, EndReason::IdleTimeout);
}

#[tokio::test]
async fn shell_exit_reaches_browser() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;
    br.send_resize(24, 80).await;
    dev.expect_open().await;
    br.send_input(b"exit\n").await; // 假 shell 对 exit 回 pty.exit

    let last = br.wait_closed().await;
    assert_eq!(last["reason"], "shell_exit");
    assert_eq!(hooks.ends()[0].reason, EndReason::ShellExit);
}

#[tokio::test]
async fn slow_browser_loses_no_data() {
    let srv = TestServer::start(Config::default()).await;
    let hooks = srv.hooks.clone();

    let mut dev = srv.connect_device().await;
    let ticket = srv.issue_ticket().await;
    let mut br = srv.connect_browser(ticket).await;
    br.send_resize(24, 80).await;
    dev.expect_open().await;

    for i in 0..50 {
        br.send_input(format!("line-{i:02}\n").as_bytes()).await;
    }
    let mut got = 0;
    for _ in 0..50 {
        // 每帧间隔 5ms：制造慢消费者，压背压链
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        if br
            .wait_for(format!("line-{:02}", got).as_bytes())
            .await
            .is_some()
        {
            got += 1;
        } else {
            break;
        }
    }
    assert_eq!(got, 50, "50 帧必须全数到达，无静默丢弃");
    br.send_close().await;
    br.wait_closed().await;
    assert_eq!(hooks.ends().len(), 1);
}
