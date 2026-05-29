mod common;

use std::sync::Arc;

use arithmetic_base::{AddRequest, CountRequest, MulRequest};
use futures::{SinkExt, StreamExt};
use identity::{InMemoryIdentityStore, RegisterRequest, IdentityV1};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use transport::{JsonCodec, ModuleMethodCall};

// ─── helpers ─────────────────────────────────────────────────────────────────

async fn setup() -> (String, Arc<InMemoryIdentityStore>) {
    common::start_signalr_server().await
}

/// Register a user and return the JWT.
async fn register_user(
    identity: &InMemoryIdentityStore,
    handle: &str,
    password: &str,
    role: &str,
) -> String {
    identity
        .register(RegisterRequest {
            handle: handle.to_string(),
            password: password.to_string(),
            role: role.to_string(),
        })
        .await
        .expect("register should succeed")
        .token
}

/// POST /hub/negotiate with the given token; returns the raw status + body.
async fn negotiate(base_url: &str, token: Option<&str>) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base_url}/hub/negotiate"));
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    req.send().await.expect("HTTP request should succeed")
}

/// Open a SignalR WebSocket connection using the provided connection_id,
/// perform the protocol handshake and return the split sink/stream.
async fn ws_connect(
    base_url: &str,
    connection_id: &str,
) -> (
    futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) {
    let ws_url = base_url
        .replace("http://", "ws://")
        + "/hub?id="
        + connection_id;
    let (ws, _) = connect_async(ws_url).await.expect("WebSocket connect should succeed");
    let (mut write, mut read) = ws.split();

    // Send SignalR handshake
    write
        .send(Message::Text(
            "{\"protocol\":\"json\",\"version\":1}\x1e".into(),
        ))
        .await
        .expect("handshake send");

    // Receive "{}\x1e"
    let hs = read
        .next()
        .await
        .expect("handshake response")
        .expect("ok message");
    let text = match &hs {
        Message::Text(t) => t.to_string(),
        _ => panic!("expected text handshake response"),
    };
    assert!(text.contains("{}"), "unexpected handshake response: {text}");

    (write, read)
}

/// Send a SignalR invocation and return the decoded completion result value.
async fn rpc_call(
    write: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    read: &mut futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    invocation_id: &str,
    module_method_call: Value,
) -> Value {
    let invocation = serde_json::json!({
        "type": 1,
        "invocationId": invocation_id,
        "target": "rpc",
        "arguments": [module_method_call],
    });
    write
        .send(Message::Text(format!("{invocation}\x1e").into()))
        .await
        .expect("invocation send");

    // Collect messages until we get a type-3 completion for our invocation_id
    loop {
        let msg = read
            .next()
            .await
            .expect("expected server message")
            .expect("ok");
        let text = match &msg {
            Message::Text(t) => t.to_string(),
            _ => continue,
        };
        for record in text.split('\x1e').filter(|s| !s.trim().is_empty()) {
            let value: Value =
                serde_json::from_str(record).expect("valid JSON record");
            if value.get("type").and_then(|t| t.as_u64()) == Some(3)
                && value.get("invocationId").and_then(|v| v.as_str())
                    == Some(invocation_id)
            {
                return value;
            }
        }
    }
}

// ─── negotiate tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn negotiate_without_token_returns_401() {
    let (base_url, _identity) = setup().await;
    let resp = negotiate(&base_url, None).await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn negotiate_with_invalid_token_returns_401() {
    let (base_url, _identity) = setup().await;
    let resp = negotiate(&base_url, Some("not.a.valid.jwt")).await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn negotiate_with_valid_token_returns_connection_id() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "alice", "pw1", "user").await;
    let resp = negotiate(&base_url, Some(&token)).await;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.expect("JSON body");
    assert!(
        body.get("connectionId").and_then(|v| v.as_str()).is_some(),
        "expected connectionId in response, got: {body}"
    );
}

// ─── WebSocket connection tests ───────────────────────────────────────────────

#[tokio::test]
async fn ws_connect_with_invalid_id_returns_401() {
    let (base_url, _identity) = setup().await;
    let ws_url = base_url.replace("http://", "ws://") + "/hub?id=not-a-real-id";
    // Server should reject – tungstenite returns an Err or a non-101 status.
    let result = connect_async(ws_url).await;
    assert!(result.is_err(), "expected WebSocket upgrade to be rejected");
}

#[tokio::test]
async fn ws_handshake_succeeds_with_valid_id() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "bob", "pw2", "user").await;
    let body: Value = negotiate(&base_url, Some(&token))
        .await
        .json()
        .await
        .unwrap();
    let connection_id = body["connectionId"].as_str().unwrap();
    let (mut write, mut read) = ws_connect(&base_url, connection_id).await;
    let _ = (&mut write, &mut read); // prevent unused-must-use warnings
}

// ─── RPC dispatch tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn signalr_add_returns_correct_result() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "carol", "pw3", "user").await;

    let body: Value = negotiate(&base_url, Some(&token))
        .await
        .json()
        .await
        .unwrap();
    let connection_id = body["connectionId"].as_str().unwrap();
    let (mut write, mut read) = ws_connect(&base_url, connection_id).await;

    let mmc =
        ModuleMethodCall::new::<JsonCodec, AddRequest>("arithmetic_v_1", "add", AddRequest { a: 7, b: 3 })
            .expect("build ModuleMethodCall");
    let args_json = serde_json::to_value(&mmc).expect("serialize");

    let completion = rpc_call(&mut write, &mut read, "inv-1", args_json).await;
    assert!(
        completion.get("error").is_none(),
        "unexpected error: {completion}"
    );
    let result = &completion["result"];
    assert_eq!(result["r"], 10, "7 + 3 should equal 10");
}

#[tokio::test]
async fn signalr_mul_returns_correct_result() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "dave", "pw4", "user").await;

    let body: Value = negotiate(&base_url, Some(&token))
        .await
        .json()
        .await
        .unwrap();
    let connection_id = body["connectionId"].as_str().unwrap();
    let (mut write, mut read) = ws_connect(&base_url, connection_id).await;

    let mmc = ModuleMethodCall::new::<JsonCodec, MulRequest>(
        "arithmetic_v_1",
        "mul",
        MulRequest { a: 6, b: 7 },
    )
    .expect("build ModuleMethodCall");
    let args_json = serde_json::to_value(&mmc).expect("serialize");

    let completion = rpc_call(&mut write, &mut read, "inv-2", args_json).await;
    assert!(
        completion.get("error").is_none(),
        "unexpected error: {completion}"
    );
    assert_eq!(completion["result"]["r"], 42);
}

#[tokio::test]
async fn signalr_count_streams_progress_items() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "eve", "pw5", "admin").await;

    let body: Value = negotiate(&base_url, Some(&token))
        .await
        .json()
        .await
        .unwrap();
    let connection_id = body["connectionId"].as_str().unwrap();
    let (mut write, mut read) = ws_connect(&base_url, connection_id).await;

    let mmc = ModuleMethodCall::new::<JsonCodec, CountRequest>(
        "arithmetic_v_1",
        "count",
        CountRequest { from: 1, to: 3 },
    )
    .expect("build ModuleMethodCall");

    let invocation = serde_json::json!({
        "type": 1,
        "invocationId": "inv-stream",
        "target": "rpc",
        "arguments": [serde_json::to_value(&mmc).unwrap()],
    });
    write
        .send(Message::Text(format!("{invocation}\x1e").into()))
        .await
        .expect("send invocation");

    let mut progress_count = 0usize;
    let mut got_completion = false;
    loop {
        let msg = read.next().await.expect("message").expect("ok");
        let text = match &msg {
            Message::Text(t) => t.to_string(),
            _ => continue,
        };
        for record in text.split('\x1e').filter(|s| !s.trim().is_empty()) {
            let value: Value = serde_json::from_str(record).unwrap();
            match value.get("type").and_then(|t| t.as_u64()) {
                Some(2) => progress_count += 1,
                Some(3) => {
                    got_completion = true;
                    assert!(
                        value.get("error").is_none(),
                        "unexpected stream error: {value}"
                    );
                }
                _ => {}
            }
        }
        if got_completion {
            break;
        }
    }

    // from=1 to=3 produces 3 steps (1, 2, 3)
    assert_eq!(progress_count, 3, "expected 3 progress items");
    assert!(got_completion);
}

#[tokio::test]
async fn signalr_unknown_target_returns_error() {
    let (base_url, identity) = setup().await;
    let token = register_user(&identity, "frank", "pw6", "user").await;

    let body: Value = negotiate(&base_url, Some(&token))
        .await
        .json()
        .await
        .unwrap();
    let connection_id = body["connectionId"].as_str().unwrap();
    let (mut write, mut read) = ws_connect(&base_url, connection_id).await;

    let invocation = serde_json::json!({
        "type": 1,
        "invocationId": "inv-bad",
        "target": "notRpc",
        "arguments": [{}],
    });
    write
        .send(Message::Text(format!("{invocation}\x1e").into()))
        .await
        .expect("send");

    let completion = 'recv: {
        loop {
            let msg = read.next().await.expect("message").expect("ok");
            let text = match &msg {
                Message::Text(t) => t.to_string(),
                _ => continue,
            };
            for record in text.split('\x1e').filter(|s| !s.trim().is_empty()) {
                let v: Value = serde_json::from_str(record).unwrap();
                if v.get("type").and_then(|t| t.as_u64()) == Some(3) {
                    break 'recv v;
                }
            }
        }
    };

    assert!(
        completion.get("error").is_some(),
        "expected error field for unknown target, got: {completion}"
    );
}

