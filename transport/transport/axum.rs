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
    http::StatusCode,
    response::IntoResponse
};

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    pub async fn handle_post(
        State(tx): State<mpsc::Sender<AxumOneShotItem>>,
        body: Bytes,
    ) -> impl IntoResponse {
        // 1. Decode request body
        let payload = match C::decode::<Payload>(&body) {
            Ok(payload) => payload,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };

        // 2. Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // 3. Send request into transport
        let item = AxumOneShotItem {
            payload,
            response: response_tx,
        };

        if tx.send(item).await.is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        // 4. Wait for server-side logic to produce response
        let response_payload = match response_rx.await {
            Ok(payload) => payload,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        // 5. Encode response and return as HTTP body
        match C::encode(&response_payload) {
            Ok(bytes) => bytes.get_bytes_forever().into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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
