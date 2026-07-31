use std::{convert::Infallible, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use crate::BearerTokenValidator;
use crate::transport::{
    IdentityTransportItem, ServerSideOneShotTransport, ServerSideOneShotTransportItem,
};
use crate::transport_types::*;
use axum::body::{Body, Bytes};
use axum::{
    extract::State,
    http::{StatusCode, header::AUTHORIZATION, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, debug_span, error, warn};

/// Axum-backed server transport for unary and streamed HTTP RPC requests.
pub struct AxumTransport<C: UnaryCodec> {
    rx: tokio::sync::Mutex<Option<mpsc::Receiver<AxumOneShotItem>>>,
    tx: mpsc::Sender<AxumOneShotItem>,
    _codec: PhantomData<C>,
}

impl<C: UnaryCodec> AxumTransport<C> {
    /// Creates a new transport with the given inbound buffering capacity.
    pub fn new(buffer: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer);

        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            _codec: PhantomData,
        }
    }
}

/// Server-side handle for a single inbound Axum RPC request.
/// Header a client may use to say what it is: `platform/version+build`, e.g.
/// `ios/1.0.2+42`.
///
/// Self-reported and unverified by design — its purpose is to answer "what is
/// actually installed out there", which is what turns retiring a compatibility
/// shim into a decision backed by evidence instead of a guess. Anything security
/// relevant belongs in the token, not here.
pub const CLIENT_INFO_HEADER: &str = "x-allex-client";

/// Reads [`CLIENT_INFO_HEADER`], bounded so a caller cannot push an arbitrary
/// string into whatever the host records.
fn client_info_of(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(CLIENT_INFO_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.chars().take(128).collect())
}

pub struct AxumOneShotItem {
    payload: Payload,
    response: tokio::sync::oneshot::Sender<AxumResponse>,
    caller_role: Option<String>,
    /// Self-reported, from `X-Allex-Client`. Never used for authorization.
    client_info: Option<String>,
}

impl ServerSideOneShotTransportItem for AxumOneShotItem {
    fn get_payload(&self) -> &Payload {
        &self.payload
    }

    async fn respond_with(self, payload: Payload) -> Result<()> {
        self.response
            .send(AxumResponse::Unary(payload))
            .map_err(|_| {
                Error::SendRequest("Could not send response, the caller dropped".to_string())
            })
    }
}

/// Extension trait for transports that can stream Axum responses.
pub trait ServerSideChunkedTransportItem {
    /// Completes the request by streaming progress and terminal frames to the client.
    fn respond_with_stream<S>(self, stream: S) -> impl Future<Output = Result<()>> + Send
    where
        S: Stream<Item = CallResponse> + Send + 'static;
}

impl ServerSideChunkedTransportItem for AxumOneShotItem {
    fn respond_with_stream<S>(self, stream: S) -> impl Future<Output = Result<()>> + Send
    where
        S: Stream<Item = CallResponse> + Send + 'static,
    {
        async move {
            self.response
                .send(AxumResponse::Stream(Box::pin(stream)))
                .map_err(|_| {
                    Error::SendRequest(
                        "Could not send streamed response, the caller dropped".to_string(),
                    )
                })
        }
    }
}

impl IdentityTransportItem for AxumOneShotItem {
    fn caller_role(&self) -> Option<&str> {
        self.caller_role.as_deref()
    }

    fn client_info(&self) -> Option<&str> {
        self.client_info.as_deref()
    }
    // Agent Change 06/04/2026: respond_with_app_error now sends AxumResponse::AppError(payload) instead of Unary(payload), enabling HTTP 422 response.

    async fn respond_with_app_error(self, payload: Payload) -> Result<()> {
        self.response
            .send(AxumResponse::AppError(payload))
            .map_err(|_| Error::SendRequest("Could not send app error response".to_string()))
    }

    // Agent Change 06/04/2026: Changed to pass error directly as TransportError variant instead of encoding to JSON payload on HTTP 200.
    async fn respond_with_transport_error(self, error: Error) -> Result<()> {
        // transport error — pass directly to handler
        self.response
            .send(AxumResponse::TransportError(error))
            .map_err(|_| Error::SendRequest("Could not send transport error response".to_string()))
    }
}

impl<C: UnaryCodec> ServerSideOneShotTransport for AxumTransport<C> {
    type Item = AxumOneShotItem;
    type ItemStream = ReceiverStream<Self::Item>;

    async fn get_incoming_stream(&self) -> Self::ItemStream {
        let mut guard = self.rx.lock().await;

        let rx = match guard.take() {
            Some(rx) => rx,
            None => {
                warn!("get_incoming_stream called more than once; returning empty stream");
                let (_, rx) = mpsc::channel(1);
                rx
            }
        };
        ReceiverStream::new(rx)
    }
}

enum AxumResponse {
    Unary(Payload),
    Stream(Pin<Box<dyn Stream<Item = CallResponse> + Send>>),
    // Agent Change 06/04/2026: Added AppError variant — carries an app-level error Payload to be returned as HTTP 422.
    AppError(Payload),
    // Agent Change 06/04/2026: Added TransportError variant — carries a transport::Error (e.g. AccessForbidden) to be returned as HTTP 403.
    TransportError(Error),
}

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    /// Axum handler for `POST /rpc`.
    pub async fn handle_post(
        State(tx): State<mpsc::Sender<AxumOneShotItem>>,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> Response {
        let request_span = debug_span!(
            "allex.transport.axum.request",
            authenticated = false,
            request_bytes = body.len(),
        );
        let _guard = request_span.enter();
        debug!("received unary axum rpc request");
        // 1. Decode request body
        let payload = match C::decode::<Payload>(&body) {
            Ok(payload) => payload,
            Err(err) => {
                warn!(error = ?err, "could not decode request payload");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Could not decode request payload: {err:?}"),
                );
            }
        };

        // 2. Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // 3. Send request into transport
        let item = AxumOneShotItem {
            payload,
            response: response_tx,
            caller_role: None,
            client_info: client_info_of(&headers),
        };

        if tx.send(item).await.is_err() {
            error!("transport receiver is not available");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Transport receiver is not available",
            );
        }

        // 4. Wait for server-side logic to produce response
        let response = match response_rx.await {
            Ok(payload) => payload,
            Err(_) => {
                error!("server-side handler dropped the response channel");
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Server-side handler dropped the response channel",
                );
            }
        };

        // 5. Encode response and return as HTTP body
        match response {
            AxumResponse::Unary(response_payload) => {
                debug!(response_kind = "unary", "sending unary axum rpc response");
                match C::encode(&response_payload) {
                    Ok(bytes) => (
                        StatusCode::OK,
                        [(CONTENT_TYPE, C::content_type())],
                        bytes.get_bytes_forever(),
                    )
                        .into_response(),
                    Err(err) => {
                        error!(error = ?err, "could not encode response payload");
                        error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &format!("Could not encode response payload: {err:?}"),
                        )
                    }
                }
            }
            AxumResponse::Stream(stream) => {
                debug!(
                    response_kind = "stream",
                    "sending streaming axum rpc response"
                );
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "application/x-ndjson")],
                    Body::from_stream(stream.filter_map(|response| async move {
                        match encode_call_response::<C>(&response) {
                            Ok(bytes) => Some(Ok::<Bytes, Infallible>(bytes)),
                            Err(e) => {
                                warn!(error = ?e, "stream encode error");
                                None
                            }
                        }
                    })),
                )
                    .into_response()
            }
            // Return the encoded application error bytes directly so non-Rust HTTP clients
            // see readable JSON instead of the transport's outer byte-array wrapper.
            AxumResponse::AppError(response_payload) => {
                debug!(
                    response_kind = "app_error",
                    "sending application error response"
                );
                app_error_response::<C>(response_payload)
            }
            // Agent Change 06/04/2026: Maps TransportError::AccessForbidden to HTTP 403 in the unauthenticated handler.
            AxumResponse::TransportError(error) => {
                warn!(response_kind = "transport_error", transport_error = ?error, "sending transport error response");
                match error {
                    Error::AccessForbidden => StatusCode::FORBIDDEN.into_response(),
                    // Calling an unregistered module is a client addressing error,
                    // not a server fault -> 404, not 500.
                    Error::ModuleNotFound => StatusCode::NOT_FOUND.into_response(),
                    _ => error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Transport error: {error:?}"),
                    ),
                }
            }
        }
    }
}

use axum::{Router, routing::post};

/// Combined state for an authenticated Axum transport.
#[derive(Clone)]
struct AuthState {
    tx: mpsc::Sender<AxumOneShotItem>,
    validator: Arc<dyn BearerTokenValidator>,
}

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    /// Builds the Axum router exposing this transport on `POST /rpc`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/rpc", post(Self::handle_post))
            .with_state(self.tx.clone())
    }

    /// Builds an Axum router that validates `Authorization: Bearer <token>` on every request
    /// and attaches the caller role to each [`AxumOneShotItem`].
    pub fn router_with_auth(&self, validator: Arc<dyn BearerTokenValidator>) -> Router {
        Router::new()
            .route("/rpc", post(Self::handle_post_auth))
            .with_state(AuthState {
                tx: self.tx.clone(),
                validator,
            })
    }

    async fn handle_post_auth(
        State(state): State<AuthState>,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> Response {
        let caller_role = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .and_then(|token| state.validator.validate_bearer(token));
        let request_span = debug_span!(
            "allex.transport.axum.request",
            authenticated = true,
            caller_role = caller_role.as_deref().unwrap_or("anonymous"),
            request_bytes = body.len(),
        );
        let _guard = request_span.enter();
        debug!("received authenticated axum rpc request");

        let payload = match C::decode::<Payload>(&body) {
            Ok(p) => p,
            Err(err) => {
                warn!(error = ?err, "could not decode request payload");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("Could not decode request payload: {err:?}"),
                );
            }
        };

        let (response_tx, response_rx) = oneshot::channel();
        let item = AxumOneShotItem {
            payload,
            response: response_tx,
            caller_role,
            client_info: client_info_of(&headers),
        };

        if state.tx.send(item).await.is_err() {
            error!("transport receiver is not available");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Transport receiver is not available",
            );
        }

        match response_rx.await {
            Ok(AxumResponse::Unary(response_payload)) => {
                debug!(response_kind = "unary", "sending unary axum rpc response");
                match C::encode(&response_payload) {
                    Ok(bytes) => (
                        StatusCode::OK,
                        [(CONTENT_TYPE, C::content_type())],
                        bytes.get_bytes_forever(),
                    )
                        .into_response(),
                    Err(err) => {
                        error!(error = ?err, "could not encode response payload");
                        error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &format!("Could not encode response payload: {err:?}"),
                        )
                    }
                }
            }
            Ok(AxumResponse::Stream(stream)) => {
                debug!(
                    response_kind = "stream",
                    "sending streaming axum rpc response"
                );
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "application/x-ndjson")],
                    Body::from_stream(stream.filter_map(|response| async move {
                        match encode_call_response::<C>(&response) {
                            Ok(bytes) => Some(Ok::<Bytes, Infallible>(bytes)),
                            Err(e) => {
                                warn!(error = ?e, "stream encode error");
                                None
                            }
                        }
                    })),
                )
                    .into_response()
            }
            Ok(AxumResponse::AppError(response_payload)) => {
                debug!(
                    response_kind = "app_error",
                    "sending application error response"
                );
                app_error_response::<C>(response_payload)
            }
            Ok(AxumResponse::TransportError(error)) => {
                warn!(response_kind = "transport_error", transport_error = ?error, "sending transport error response");
                match error {
                    Error::AccessForbidden => StatusCode::FORBIDDEN.into_response(),
                    // Calling an unregistered module is a client addressing error,
                    // not a server fault -> 404, not 500.
                    Error::ModuleNotFound => StatusCode::NOT_FOUND.into_response(),
                    _ => error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("Transport error: {error:?}"),
                    ),
                }
            }
            Err(_) => {
                error!("server-side handler dropped the response channel");
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Server-side handler dropped the response channel",
                )
            }
        }
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_string()).into_response()
}

fn app_error_response<C: UnaryCodec>(payload: Payload) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        [(CONTENT_TYPE, C::content_type())],
        payload.get_bytes_forever(),
    )
        .into_response()
}

fn encode_call_response<C: UnaryCodec>(response: &CallResponse) -> Result<Bytes> {
    let mut bytes = C::encode(response)?.get_bytes_forever();
    bytes.push(b'\n');
    Ok(Bytes::from(bytes))
}
