use std::sync::Arc;

use arithmetic_base::ArithmeticV1MethodResponder;
use execution::{ModuleExecutor, Registry};
use futures::StreamExt;
use transport::{
    Error, JsonCodec, ModuleMethodCall, ServerSideSerialTransport, ServerSideSerialTransportItem,
    WebSocketSerialServer,
};

mod arithmetic_server_impl;
use arithmetic_server_impl::MyArithmetic;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let registry = Arc::new(Registry::new());
    let responder = Arc::new(
        ArithmeticV1MethodResponder::<MyArithmetic, JsonCodec>::new(MyArithmetic),
    );
    registry.register_module(responder.clone());

    let transport = WebSocketSerialServer::<JsonCodec>::bind("127.0.0.1:3310", 128).await?;
    println!("WebSocket arithmetic server listening on ws://{}", transport.local_addr());

    let mut stream = transport.get_incoming_stream().await;

    while let Some(item) = stream.next().await {
        let call = match item.get_frame().get_payload().decode::<JsonCodec, ModuleMethodCall>() {
            Ok(call) => call,
            Err(error) => {
                let _ = item.respond_with_transport_error(error).await;
                continue;
            }
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
            Ok(payload) => {
                let _ = item.respond_with(payload).await;
            }
            Err(Error::App(payload)) => {
                let _ = item.respond_with_app_error(payload).await;
            }
            Err(error) => {
                let _ = item.respond_with_transport_error(error).await;
            }
        }
    }

    Ok(())
}
