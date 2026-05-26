use std::marker::PhantomData;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use crate::transport_types::*;
use crate::transport::{ServerSideOneShotTransport, ServerSideOneShotTransportItem};

pub struct AxumTransport<C: UnaryCodec> {
    rx: tokio::sync::Mutex<Option<mpsc::Receiver<AxumOneShotItem>>>,
    tx: mpsc::Sender<AxumOneShotItem>,
    _codec: PhantomData<C>,
}

impl<C: UnaryCodec> AxumTransport<C> {
    pub fn new(buffer: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer);

        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            _codec: PhantomData,
        }
    }
}

pub struct AxumOneShotItem {
    payload: Payload,
    response: tokio::sync::oneshot::Sender<Payload>,
}

impl ServerSideOneShotTransportItem for AxumOneShotItem {
    fn get_payload(&self) -> &Payload {
        &self.payload
    }

    async fn respond_with(
        self,
        payload: Payload,
    ) -> Result<()> {
        self.response
            .send(payload)
            .map_err(|e| Error::SendRequest(format!("Could not send {} bytes, the caller dropped", e.len())))
    }
}

impl<C: UnaryCodec> ServerSideOneShotTransport for AxumTransport<C> {
    type Item = AxumOneShotItem;
    type ItemStream = ReceiverStream<Self::Item>;

    async fn get_incoming_stream(&self) -> Self::ItemStream {
        let mut guard = self.rx.lock().await;

        let rx = guard
            .take()
            .expect("incoming stream already taken");

        ReceiverStream::new(rx)
    }
}


use axum::{
    body::Bytes,
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response}
};

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    pub async fn handle_post(
        State(tx): State<mpsc::Sender<AxumOneShotItem>>,
        body: Bytes,
    ) -> impl IntoResponse {
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
        let response_payload = match response_rx.await {
            Ok(payload) => payload,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Server-side handler dropped the response channel",
                );
            }
        };

        // 5. Encode response and return as HTTP body
        match C::encode(&response_payload) {
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
        }
    }
}

use axum::{Router, routing::post};

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    pub fn router(&self) -> Router {
        Router::new()
            .route("/rpc", post(Self::handle_post))
            .with_state(self.tx.clone())
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_string()).into_response()
}
