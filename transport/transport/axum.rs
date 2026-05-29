use std::{convert::Infallible, future::Future, marker::PhantomData, pin::Pin};

use crate::transport::{ServerSideOneShotTransport, ServerSideOneShotTransportItem};
use crate::transport_types::*;
use axum::body::{Body, Bytes};
use axum::{
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

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
pub struct AxumOneShotItem {
    payload: Payload,
    response: tokio::sync::oneshot::Sender<AxumResponse>,
}

impl ServerSideOneShotTransportItem for AxumOneShotItem {
    fn get_payload(&self) -> &Payload {
        &self.payload
    }

    async fn respond_with(self, payload: Payload) -> Result<()> {
        self.response
            .send(AxumResponse::Unary(payload))
            .map_err(|_| Error::SendRequest(format!("Could not send response, the caller dropped")))
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

impl<C: UnaryCodec> ServerSideOneShotTransport for AxumTransport<C> {
    type Item = AxumOneShotItem;
    type ItemStream = ReceiverStream<Self::Item>;

    async fn get_incoming_stream(&self) -> Self::ItemStream {
        let mut guard = self.rx.lock().await;

        let rx = guard.take().expect("incoming stream already taken");

        ReceiverStream::new(rx)
    }
}

enum AxumResponse {
    Unary(Payload),
    Stream(Pin<Box<dyn Stream<Item = CallResponse> + Send>>),
}

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    /// Axum handler for `POST /rpc`.
    pub async fn handle_post(
        State(tx): State<mpsc::Sender<AxumOneShotItem>>,
        body: Bytes,
    ) -> Response {
        // 1. Decode request body
        let payload = match C::decode::<Payload>(&body) {
            Ok(payload) => payload,
            Err(err) => {
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
        };

        if tx.send(item).await.is_err() {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Transport receiver is not available",
            );
        }

        // 4. Wait for server-side logic to produce response
        let response = match response_rx.await {
            Ok(payload) => payload,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Server-side handler dropped the response channel",
                );
            }
        };

        // 5. Encode response and return as HTTP body
        match response {
            AxumResponse::Unary(response_payload) => match C::encode(&response_payload) {
                Ok(bytes) => (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "application/json")],
                    bytes.get_bytes_forever(),
                )
                    .into_response(),
                Err(err) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Could not encode response payload: {err:?}"),
                ),
            },
            AxumResponse::Stream(stream) => (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-ndjson")],
                Body::from_stream(stream.filter_map(|response| async move {
                    encode_call_response::<C>(&response)
                        .ok()
                        .map(|bytes| Ok::<Bytes, Infallible>(bytes))
                })),
            )
                .into_response(),
        }
    }
}

use axum::{Router, routing::post};

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    /// Builds the Axum router exposing this transport on `POST /rpc`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/rpc", post(Self::handle_post))
            .with_state(self.tx.clone())
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_string()).into_response()
}

fn encode_call_response<C: UnaryCodec>(response: &CallResponse) -> Result<Bytes> {
    let mut bytes = C::encode(response)?.get_bytes_forever();
    bytes.push(b'\n');
    Ok(Bytes::from(bytes))
}
