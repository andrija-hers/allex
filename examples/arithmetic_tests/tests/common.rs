#![allow(dead_code)]
//! Shared test helpers: server implementation and server startup utilities.

use std::sync::Arc;

use arithmetic_base::ArithmeticV1MethodResponder;
use execution::{ModuleExecutor, Registry};
use futures::StreamExt;
use progressable_future::{ProgressableFuture, ProgressSender};
use tokio::{net::TcpListener, time::{Duration, sleep}};
use transport::{
    AxumTransport, JsonCodec, ModuleMethodCall, ServerSideChunkedTransportItem,
    ServerSideOneShotTransport, ServerSideOneShotTransportItem,
    ServerSideSerialTransport, ServerSideSerialTransportItem,
    WebSocketSerialServer, Error,
};
use arithmetic_base::{
    ArithmeticV1, AddRequest, AddResult, ArithmeticError,
    CountRequest, CountStep, MulRequest, MulResult, StepsMade,
};

pub struct MyArithmetic;

impl ArithmeticV1 for MyArithmetic {
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError> {
        Ok(AddResult { r: req.a + req.b })
    }

    async fn mul(&self, req: MulRequest) -> Result<MulResult, ArithmeticError> {
        Ok(MulResult { r: req.a * req.b })
    }

    fn count(&self, req: CountRequest) -> ProgressableFuture<CountStep, StepsMade, ArithmeticError> {
        ProgressableFuture::new(move |sender: ProgressSender<CountStep>| async move {
            let step = if req.from <= req.to { 1_i32 } else { -1_i32 };
            let mut current = req.from;
            let mut total = 0_i32;
            loop {
                sender.progress(CountStep { currently_at: current });
                total = total.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                if current == req.to { break; }
                sleep(Duration::from_millis(5)).await;
                current = current.checked_add(step).ok_or(ArithmeticError::Overflow)?;
            }
            Ok(StepsMade { total })
        })
    }
}

fn make_registry_and_responder() -> (
    Arc<Registry>,
    Arc<ArithmeticV1MethodResponder<MyArithmetic, JsonCodec>>,
) {
    let registry = Arc::new(Registry::new());
    let responder = Arc::new(
        ArithmeticV1MethodResponder::<MyArithmetic, JsonCodec>::new(MyArithmetic),
    );
    registry.register_module(responder.clone());
    (registry, responder)
}

/// Starts an axum/HTTP server on a free port and returns its base URL.
pub async fn start_http_server() -> String {
    let (registry, responder) = make_registry_and_responder();
    let transport = AxumTransport::<JsonCodec>::new(128);
    let app = transport.router();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut stream = transport.get_incoming_stream().await;
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let call = match item.get_payload().decode::<JsonCodec, ModuleMethodCall>() {
                Ok(c) => c,
                Err(e) => { let _ = item.respond_with(e.surely_encode::<JsonCodec>()).await; continue; }
            };

            if let Some(response_stream) =
                responder.execute_streaming(call.get_method(), call.get_payload().clone())
            {
                let _ = item.respond_with_stream(response_stream).await;
                continue;
            }

            let response = registry
                .execute(call.get_module(), call.get_method(), call.get_payload().clone())
                .await;
            let payload = match response {
                Ok(p) => p,
                Err(e) => e.surely_encode::<JsonCodec>(),
            };
            let _ = item.respond_with(payload).await;
        }
    });

    format!("http://{}", address)
}

/// Starts a WebSocket serial server on a free port and returns its address.
pub async fn start_websocket_server() -> std::net::SocketAddr {
    let (registry, responder) = make_registry_and_responder();
    let transport = WebSocketSerialServer::<JsonCodec>::bind("127.0.0.1:0", 128)
        .await
        .expect("server should bind");
    let address = transport.local_addr();

    let mut stream = transport.get_incoming_stream().await;
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let call = match item.get_frame().get_payload().decode::<JsonCodec, ModuleMethodCall>() {
                Ok(c) => c,
                Err(e) => { let _ = item.respond_with_transport_error(e).await; continue; }
            };

            if let Some(response_stream) =
                responder.execute_streaming(call.get_method(), call.get_payload().clone())
            {
                let _ = item.respond_with_stream(response_stream).await;
                continue;
            }

            match registry
                .execute(call.get_module(), call.get_method(), call.get_payload().clone())
                .await
            {
                Ok(payload) => { let _ = item.respond_with(payload).await; }
                Err(Error::App(payload)) => { let _ = item.respond_with_app_error(payload).await; }
                Err(error) => { let _ = item.respond_with_transport_error(error).await; }
            }
        }
    });

    address
}
