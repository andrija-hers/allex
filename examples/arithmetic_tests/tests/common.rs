#![allow(dead_code)]
//! Shared test helpers: server implementation and server startup utilities.

use std::sync::Arc;

use arithmetic_base::{
    AddRequest, AddResult, ArithmeticError, ArithmeticV1, CountRequest, CountStep, MulRequest,
    MulResult, StepsMade,
};
use arithmetic_base::{ArithmeticV1MethodPermissions, ArithmeticV1MethodResponder};
use execution::{ModuleExecutor, Registry};
use futures::StreamExt;
use identity::InMemoryIdentityStore;
use progressable_future::{ProgressSender, ProgressableFuture};
use tokio::{
    net::TcpListener,
    time::{Duration, sleep},
};
use transport::{
    AxumTransport, BearerTokenValidator, Error, IdentityTransportItem, JsonCodec, ModuleMethodCall,
    ServerSideChunkedTransportItem, ServerSideOneShotTransport, ServerSideOneShotTransportItem,
    ServerSideSerialTransport, ServerSideSerialTransportItem, SignalRAxumTransport,
    WebSocketSerialServer,
};

pub struct MyArithmetic;

impl ArithmeticV1 for MyArithmetic {
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError> {
        Ok(AddResult { r: req.a + req.b })
    }

    async fn mul(&self, req: MulRequest) -> Result<MulResult, ArithmeticError> {
        Ok(MulResult { r: req.a * req.b })
    }

    fn count(
        &self,
        req: CountRequest,
    ) -> ProgressableFuture<CountStep, StepsMade, ArithmeticError> {
        ProgressableFuture::new(move |sender: ProgressSender<CountStep>| async move {
            let step = if req.from <= req.to { 1_i32 } else { -1_i32 };
            let mut current = req.from;
            let mut total = 0_i32;
            loop {
                sender.progress(CountStep {
                    currently_at: current,
                });
                total = total.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                if current == req.to {
                    break;
                }
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
    let responder = Arc::new(ArithmeticV1MethodResponder::<MyArithmetic, JsonCodec>::new(
        MyArithmetic,
    ));
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
                Err(e) => {
                    let _ = item.respond_with(e.surely_encode::<JsonCodec>()).await;
                    continue;
                }
            };

            // Resolve the minimum required role for this method so transport-layer
            // tests bypass authentication checks without needing a real identity.
            let method_str = call.get_method();
            let role: Option<&str> =
                if arithmetic_base::ArithmeticV1MethodPermissions::is_public(method_str) {
                    None
                } else {
                    arithmetic_base::ArithmeticV1MethodPermissions::allowed_roles(method_str)
                        .and_then(|roles| roles.first().copied())
                };
            if let Some(response_stream) =
                responder.execute_streaming(method_str, call.get_payload().clone(), role)
            {
                let _ = item.respond_with_stream(response_stream).await;
                continue;
            }
            let response = registry
                .execute(
                    call.get_module(),
                    method_str,
                    call.get_payload().clone(),
                    role,
                )
                .await;
            match response {
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
            let call = match item
                .get_frame()
                .get_payload()
                .decode::<JsonCodec, ModuleMethodCall>()
            {
                Ok(c) => c,
                Err(e) => {
                    let _ = item.respond_with_transport_error(e).await;
                    continue;
                }
            };

            let ws_method = call.get_method();
            let ws_role: Option<&str> = if ArithmeticV1MethodPermissions::is_public(ws_method) {
                None
            } else {
                ArithmeticV1MethodPermissions::allowed_roles(ws_method)
                    .and_then(|roles| roles.first().copied())
            };
            if let Some(response_stream) =
                responder.execute_streaming(ws_method, call.get_payload().clone(), ws_role)
            {
                let _ = item.respond_with_stream(response_stream).await;
                continue;
            }
            match registry
                .execute(
                    call.get_module(),
                    ws_method,
                    call.get_payload().clone(),
                    ws_role,
                )
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
    });

    address
}

/// Starts a SignalR server (negotiate + WebSocket) on a free port.
///
/// Returns `(base_url, identity_store)`.  Tests can call
/// `identity_store.register(...)` / `identity_store.login(...)` to obtain
/// JWTs that can then be used with the negotiate endpoint.
pub async fn start_signalr_server() -> (String, Arc<InMemoryIdentityStore>) {
    let identity = Arc::new(InMemoryIdentityStore::new("test-secret-key"));

    let transport =
        SignalRAxumTransport::new(identity.clone() as Arc<dyn BearerTokenValidator>, 128);
    let app = transport.router();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (registry, responder) = make_registry_and_responder();
    let mut stream = transport.get_incoming_stream().await;
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let caller_role = item.caller_role().map(|s| s.to_string());
            let call = match item.get_payload().decode::<JsonCodec, ModuleMethodCall>() {
                Ok(c) => c,
                Err(e) => {
                    let _ = item.respond_with_transport_error(e).await;
                    continue;
                }
            };

            if let Some(response_stream) = responder.execute_streaming(
                call.get_method(),
                call.get_payload().clone(),
                caller_role.as_deref(),
            ) {
                let _ = item.respond_with_stream(response_stream).await;
                continue;
            }

            match registry
                .execute(
                    call.get_module(),
                    call.get_method(),
                    call.get_payload().clone(),
                    caller_role.as_deref(),
                )
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
    });

    (format!("http://{address}"), identity)
}
