use std::marker::PhantomData;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::types::*;
use crate::transport::{Frame, ServerSideTransport};

pub struct AxumTransport<C: UnaryCodec> {
    rx: tokio::sync::Mutex<Option<mpsc::Receiver<Frame>>>,
    tx: mpsc::Sender<Frame>,
    _codec: PhantomData<C>,
}

impl<C: UnaryCodec> AxumTransport<C> {
    pub fn new(buffer: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer);

        Self {
            tx,
            rx: tokio::sync::Mutex::new(Some(rx)),
            _codec: PhantomData,
        }
    }
}


impl<C: UnaryCodec> ServerSideTransport for AxumTransport<C> {
    type FrameStream = ReceiverStream<Frame>;

    fn get_incoming_stream(&self) -> Self::FrameStream {
        let mut guard = self.rx.blocking_lock();

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
};

impl<C: UnaryCodec + Send + Sync + 'static> AxumTransport<C> {
    pub async fn handle_post(
        State(tx): State<mpsc::Sender<Frame>>,
        body: Bytes,
    ) -> StatusCode {
        match C::decode::<Frame>(&body) {
            Ok(frame) => {
                match tx.send(frame).await {
                    Ok(_) => StatusCode::OK,
                    Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                }
            }
            Err(_) => StatusCode::BAD_REQUEST,
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
