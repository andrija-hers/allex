use std::sync::Arc;
use tokio::net::TcpListener;

use transport::{JsonCodec, ModuleMethodCall, ServerSideOneShotTransportItem, ServerSideOneShotTransport, AxumTransport};
use execution::{Registry, ModuleExecutor};
use arithmetic_base::{ArithmeticV1MethodResponder};
use crate::MyArithmetic;

pub async fn run_axum_one_shot() {
    let registry = Arc::new(Registry::new());
    let responder = Arc::new(ArithmeticV1MethodResponder::<MyArithmetic, JsonCodec>::new(MyArithmetic));
    registry.register_module(responder);

    let transport = AxumTransport::<JsonCodec>::new(128);
    let app = transport.router();

    let listener = TcpListener::bind("127.0.0.1:3300")
        .await
        .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap();
    });

    let mut stream = transport.get_incoming_stream().await;

    use futures::StreamExt;

    while let Some(item) = stream.next().await {
        let call = match item.get_payload().decode::<JsonCodec, ModuleMethodCall>() {
            Ok(c) => c,
            Err(e) => {
                let _ = item.respond_with(e.surely_encode::<JsonCodec>()).await;
                continue;
            }
        };

        let response = registry
            .execute(call.get_module(), call.get_method(), call.get_payload().clone())
            .await;

        let response_payload = match response {
            Ok(payload) => payload,
            Err(e) => e.surely_encode::<JsonCodec>(),
        };

        let _ = item.respond_with(response_payload).await;
    }
}