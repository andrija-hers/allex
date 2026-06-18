use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{SinkExt, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration as TokioDuration, timeout};
use uuid::Uuid;

use crate::{CallResponse, Error, Payload, Result};
use crate::{
    IdentityOneShotTransport, IdentityTransportItem, ServerSideChunkedTransportItem,
    ServerSideOneShotTransport, ServerSideOneShotTransportItem,
};

/// Implemented by anything that can verify a Bearer JWT and return the authenticated caller.
pub trait BearerTokenValidator: Send + Sync {
    /// Validates a bearer token and returns `handle<RS>role` on success.
    fn validate_bearer(&self, token: &str) -> Option<String>;
}

// ─── negotiate response ──────────────────────────────────────────────────────

/// JSON body returned by the SignalR negotiate endpoint.
#[derive(Debug, Serialize)]
pub struct SignalRNegotiateResponse {
    #[serde(rename = "connectionId")]
    /// One-time connection identifier to present on the WebSocket upgrade request.
    pub connection_id: String,
    /// Non-null enables future redirect / multi-server scaling.
    pub url: Option<String>,
}

// ─── internal outgoing message ───────────────────────────────────────────────

enum SignalROutgoing {
    StreamItem {
        invocation_id: String,
        item: serde_json::Value,
    },
    Completion {
        invocation_id: String,
        result: Option<serde_json::Value>,
        error: Option<String>,
    },
    // Part of the SignalR protocol model; this placeholder transport never emits
    // it yet, but the encoder handles it.
    #[allow(dead_code)]
    Pong,
}

// ─── public item handed to the server dispatch loop ─────────────────────────

/// A single inbound SignalR invocation, ready to be dispatched and responded to.
pub struct SignalRItem {
    invocation_id: String,
    payload: Payload,
    role: Option<String>,
    outgoing: mpsc::UnboundedSender<SignalROutgoing>,
}

impl ServerSideOneShotTransportItem for SignalRItem {
    fn get_payload(&self) -> &Payload {
        &self.payload
    }

    async fn respond_with(self, payload: Payload) -> Result<()> {
        let result = serde_json::from_slice::<serde_json::Value>(payload.get_bytes())
            .map_err(|e| Error::Serialize(e.to_string()))?;
        self.outgoing
            .send(SignalROutgoing::Completion {
                invocation_id: self.invocation_id,
                result: Some(result),
                error: None,
            })
            .map_err(|_| Error::SendRequest("SignalR outgoing channel closed".into()))
    }
}

impl ServerSideChunkedTransportItem for SignalRItem {
    async fn respond_with_stream<S>(self, stream: S) -> Result<()>
    where
        S: futures::Stream<Item = CallResponse> + Send + 'static,
    {
        let outgoing = self.outgoing;
        let invocation_id = self.invocation_id;

        pin_mut!(stream);
        let mut sent_terminal = false;
        while let Some(response) = stream.next().await {
            match response {
                CallResponse::Progress(payload) => {
                    let item = serde_json::from_slice::<serde_json::Value>(payload.get_bytes())
                        .map_err(|e| Error::Serialize(e.to_string()))?;
                    if outgoing
                        .send(SignalROutgoing::StreamItem {
                            invocation_id: invocation_id.clone(),
                            item,
                        })
                        .is_err()
                    {
                        return Err(Error::SendRequest("SignalR outgoing channel closed".into()));
                    }
                }
                CallResponse::Success(payload) => {
                    let result = serde_json::from_slice::<serde_json::Value>(payload.get_bytes())
                        .map_err(|e| Error::Serialize(e.to_string()))?;
                    let _ = outgoing.send(SignalROutgoing::Completion {
                        invocation_id: invocation_id.clone(),
                        result: Some(result),
                        error: None,
                    });
                    sent_terminal = true;
                    break;
                }
                CallResponse::Error(payload) => {
                    let error = serde_json::from_slice::<serde_json::Value>(payload.get_bytes())
                        .map(|v| v.to_string())
                        .unwrap_or_else(|_| {
                            String::from_utf8_lossy(payload.get_bytes()).into_owned()
                        });
                    let _ = outgoing.send(SignalROutgoing::Completion {
                        invocation_id: invocation_id.clone(),
                        result: None,
                        error: Some(error),
                    });
                    sent_terminal = true;
                    break;
                }
            }
        }

        if !sent_terminal {
            let _ = outgoing.send(SignalROutgoing::Completion {
                invocation_id,
                result: None,
                error: Some("streaming handler ended without a terminal response".to_string()),
            });
            return Err(Error::ReceiveResponse(
                "streaming handler ended without a terminal response".to_string(),
            ));
        }
        Ok(())
    }
}

impl IdentityTransportItem for SignalRItem {
    fn caller_role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    async fn respond_with_app_error(self, payload: Payload) -> Result<()> {
        let error = serde_json::from_slice::<serde_json::Value>(payload.get_bytes())
            .map(|v| v.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(payload.get_bytes()).into_owned());
        self.outgoing
            .send(SignalROutgoing::Completion {
                invocation_id: self.invocation_id,
                result: None,
                error: Some(error),
            })
            .map_err(|_| Error::SendRequest("SignalR outgoing channel closed".into()))
    }

    async fn respond_with_transport_error(self, error: Error) -> Result<()> {
        self.outgoing
            .send(SignalROutgoing::Completion {
                invocation_id: self.invocation_id,
                result: None,
                error: Some(format!("{error:?}")),
            })
            .map_err(|_| Error::SendRequest("SignalR outgoing channel closed".into()))
    }
}

// ─── axum state ──────────────────────────────────────────────────────────────

struct SignalRAxumState {
    /// Pending sessions: connection_id → (role, created_at) — 30s single-use ticket.
    sessions: Arc<Mutex<HashMap<String, (String, Instant)>>>,
    validator: Arc<dyn BearerTokenValidator>,
    incoming_tx: mpsc::Sender<SignalRItem>,
}
// ─── transport ───────────────────────────────────────────────────────────────

/// A combined HTTP-negotiate + WebSocket SignalR transport.
///
/// 1. `POST /hub/negotiate` validates the `Authorization: Bearer` header and
///    returns a one-time `connectionId`.
/// 2. `GET /hub?id=<connectionId>` performs the WebSocket upgrade, runs the
///    SignalR handshake, then forwards every invocation (target must be `"rpc"`)
pub struct SignalRAxumTransport {
    sessions: Arc<Mutex<HashMap<String, (String, Instant)>>>,
    validator: Arc<dyn BearerTokenValidator>,
    incoming_tx: mpsc::Sender<SignalRItem>,
    // std::sync::Mutex so extraction is synchronous and doesn't borrow self into the stream.
    incoming_rx: std::sync::Mutex<Option<mpsc::Receiver<SignalRItem>>>,
}
impl SignalRAxumTransport {
    /// Creates a SignalR transport backed by the supplied bearer-token validator.
    pub fn new(validator: Arc<dyn BearerTokenValidator>, buffer: usize) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::channel(buffer);
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            validator,
            incoming_tx,
            incoming_rx: std::sync::Mutex::new(Some(incoming_rx)),
        }
    }

    /// Builds the Axum router exposing `/hub/negotiate` and `/hub`.
    pub fn router(&self) -> Router {
        let state = Arc::new(SignalRAxumState {
            sessions: self.sessions.clone(),
            validator: self.validator.clone(),
            incoming_tx: self.incoming_tx.clone(),
        });
        Router::new()
            .route("/hub/negotiate", post(handle_negotiate))
            .route("/hub", get(handle_ws_upgrade))
            .with_state(state)
    }
}

impl ServerSideOneShotTransport for SignalRAxumTransport {
    type Item = SignalRItem;
    type ItemStream = tokio_stream::wrappers::ReceiverStream<SignalRItem>;

    async fn get_incoming_stream(&self) -> Self::ItemStream {
        let rx = self
            .incoming_rx
            .lock()
            .unwrap()
            .take()
            .expect("incoming stream already taken");
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }
}

impl IdentityOneShotTransport for SignalRAxumTransport {}

// ─── negotiate handler ───────────────────────────────────────────────────────

async fn handle_negotiate(
    headers: HeaderMap,
    State(state): State<Arc<SignalRAxumState>>,
) -> Response {
    let token = match extract_bearer(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "Missing Authorization: Bearer header",
            )
                .into_response();
        }
    };
    let role = match state.validator.validate_bearer(&token) {
        Some(r) => r,
        None => {
            return (StatusCode::UNAUTHORIZED, "Invalid or expired token").into_response();
        }
    };
    let connection_id = Uuid::new_v4().to_string();
    let mut sessions = state.sessions.lock().await;
    // Prune sessions older than 30 seconds to prevent unbounded growth.
    let now = Instant::now();
    sessions.retain(|_, (_, created)| now.duration_since(*created) < Duration::from_secs(30));
    sessions.insert(connection_id.clone(), (role, now));
    drop(sessions);
    Json(SignalRNegotiateResponse {
        connection_id,
        url: None,
    })
    .into_response()
}

// ─── WebSocket upgrade handler ────────────────────────────────────────────────

async fn handle_ws_upgrade(
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<SignalRAxumState>>,
) -> Response {
    let connection_id = match params.get("id") {
        Some(id) => id.clone(),
        None => return (StatusCode::BAD_REQUEST, "Missing id query parameter").into_response(),
    };
    let role = match state.sessions.lock().await.remove(&connection_id) {
        Some((r, created)) if Instant::now().duration_since(created) < Duration::from_secs(30) => r,
        Some(_) => return (StatusCode::UNAUTHORIZED, "Connection id has expired").into_response(),
        None => return (StatusCode::UNAUTHORIZED, "Unknown connection id").into_response(),
    };
    ws.on_upgrade(move |socket| handle_socket(socket, role, state.incoming_tx.clone()))
}

// ─── WebSocket connection loop ────────────────────────────────────────────────

async fn handle_socket(socket: WebSocket, role: String, incoming_tx: mpsc::Sender<SignalRItem>) {
    let (mut write, mut read) = socket.split();

    // SignalR handshake: client sends {"protocol":"json","version":1}\x1e
    let first = match read.next().await {
        Some(Ok(msg)) => msg,
        _ => return,
    };
    let text = message_to_str(&first);
    let first_record = text.split('\x1e').next().unwrap_or("").trim();
    if serde_json::from_str::<SignalRHandshake>(first_record).is_err() {
        return;
    }
    if write.send(Message::Text("{}\x1e".into())).await.is_err() {
        return;
    }

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<SignalROutgoing>();
    // Close connection if no activity for 60 seconds (prevents zombie/leaked connections).
    loop {
        tokio::select! {
            msg = timeout(TokioDuration::from_secs(60), read.next()) => {
                match msg {
                    Ok(Some(Ok(msg))) => {
                        let text = message_to_str(&msg);
                        for record in text.split('\x1e').filter(|s| !s.trim().is_empty()) {
                            dispatch_record(record, &role, &incoming_tx, &out_tx).await;
                        }
                    }
                    Ok(None) | Ok(Some(Err(_))) => break, // connection closed or error
                    Err(_) => break, // 60s inactivity timeout
                }
            }
            outgoing = out_rx.recv() => {
                match outgoing {
                    Some(msg) => {
                        if write.send(Message::Text(encode_outgoing(msg).into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

// ─── per-record dispatcher ────────────────────────────────────────────────────

async fn dispatch_record(
    record: &str,
    role: &str,
    incoming_tx: &mpsc::Sender<SignalRItem>,
    out_tx: &mpsc::UnboundedSender<SignalROutgoing>,
) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(record) else {
        return;
    };
    let msg_type = value.get("type").and_then(|t| t.as_u64()).unwrap_or(0);

    match msg_type {
        // Invocation (client → server)
        1 => {
            let invocation_id = value
                .get("invocationId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let target = value.get("target").and_then(|v| v.as_str()).unwrap_or("");
            if target != "rpc" {
                let _ = out_tx.send(SignalROutgoing::Completion {
                    invocation_id,
                    result: None,
                    error: Some(format!("unknown target: {target}")),
                });
                return;
            }
            let args = value
                .get("arguments")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if args.is_empty() {
                let _ = out_tx.send(SignalROutgoing::Completion {
                    invocation_id,
                    result: None,
                    error: Some("missing rpc arguments".into()),
                });
                return;
            }
            // Re-serialise arguments[0] to bytes so the existing Payload/codec
            // pipeline can decode it as a ModuleMethodCall.
            let payload_bytes = match serde_json::to_vec(&args[0]) {
                Ok(b) => b,
                Err(e) => {
                    let _ = out_tx.send(SignalROutgoing::Completion {
                        invocation_id,
                        result: None,
                        error: Some(format!("failed to serialise argument: {e}")),
                    });
                    return;
                }
            };
            let item = SignalRItem {
                invocation_id,
                payload: Payload::new(&payload_bytes),
                role: Some(role.to_string()),
                outgoing: out_tx.clone(),
            };
            let _ = incoming_tx.send(item).await;
        }
        // Ping – just ignore (the server sends no mandatory pong in the JSON protocol)
        6 => {}
        _ => {}
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SignalRHandshake {
    #[allow(dead_code)]
    protocol: String,
    #[allow(dead_code)]
    version: u32,
}

fn message_to_str(msg: &Message) -> String {
    match msg {
        Message::Text(t) => t.to_string(),
        Message::Binary(b) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    }
}

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let val = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    val.strip_prefix("Bearer ").map(|s| s.to_string())
}

fn encode_outgoing(msg: SignalROutgoing) -> String {
    let json = match msg {
        SignalROutgoing::StreamItem {
            invocation_id,
            item,
        } => serde_json::json!({
            "type": 2,
            "invocationId": invocation_id,
            "item": item,
        }),
        SignalROutgoing::Completion {
            invocation_id,
            result,
            error,
        } => {
            let mut obj = serde_json::json!({
                "type": 3,
                "invocationId": invocation_id,
            });
            if let Some(r) = result {
                obj["result"] = r;
            }
            if let Some(e) = error {
                obj["error"] = serde_json::Value::String(e);
            }
            obj
        }
        SignalROutgoing::Pong => serde_json::json!({ "type": 6 }),
    };
    format!("{json}\x1e")
}
