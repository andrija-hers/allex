//! `ServiceHost` — the batteries-included host builder that owns allex's one correct wiring.
//!
//! # Why this exists
//!
//! Every allex backend hand-writes the same handful of lines to stand up an HTTP RPC server: build
//! an `execution::Registry`, register each generated `<Trait>MethodResponder`, create an
//! `AxumTransport`, build a router with `router_with_auth`, then **`tokio::spawn(serve_dispatch(..))`
//! after the router borrows the transport but before the transport is moved.** That last step is the
//! footgun — forget the spawn and every request hangs. [`ServiceHost`] writes those lines once,
//! correctly, so a new project never re-derives (or mis-orders) them.
//!
//! # It hands back the router — it does not seal it
//!
//! Real backends bolt their own surface onto the RPC router: a CORS layer, `/health`, static-file
//! serving, a cookie-auth REST bridge. So this is an **extensible core**, not a black box: register
//! your modules, then take back the `axum::Router` (already wired to a spawned dispatch loop) and
//! add whatever you like before serving.
//!
//! ```
//! use allex_host::ServiceHost;
//! use transport::JsonCodec;
//!
//! let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! rt.block_on(async {
//!     // Register your `FooV1MethodResponder`s with `.register(..)`; here we build an empty host.
//!     let service = ServiceHost::<JsonCodec>::new(); // .register(Arc::new(FooV1MethodResponder::new(svc)))
//!     let (router, _dispatch) = service.into_router().into_parts();
//!     // `router` is a plain axum Router — layer CORS, add routes, then `axum::serve` it.
//!     let _ = router;
//! });
//! ```
//!
//! With auth, pass a `transport::BearerTokenValidator` (any allex identity store is one) to
//! [`ServiceHost::into_router_with_auth`]. The returned [`RpcService`] offers
//! [`map_router`](RpcService::map_router) / [`with_health`](RpcService::with_health) to extend the
//! router and [`serve`](RpcService::serve) to run it — racing the HTTP server against the dispatch
//! loop so if either ends, the process shuts down cleanly.
//!
//! # Startup boilerplate
//!
//! The [`bootstrap`] module owns the other copy-pasted prologue — JWT-secret loading (with a
//! persisted dev fallback), TTL/env parsing, `.env` loading, and bind-address resolution — so a new
//! `main.rs` reads as configuration, not boilerplate.

pub mod bootstrap;

use std::marker::PhantomData;
use std::sync::Arc;

use axum::Router;
use execution::{MethodExecutor, Registry, StreamingMethodExecutor};
use serve::serve_dispatch;
use tokio::task::JoinHandle;
use transport::{AxumTransport, BearerTokenValidator, JsonCodec, UnaryCodec};

/// Default inbound buffering capacity for the transport channel.
pub const DEFAULT_BUFFER: usize = 1024;

/// Collects module responders and stands them up as an axum HTTP RPC service, owning the
/// `Registry` + `AxumTransport` + `serve_dispatch` wiring (and its spawn-order footgun).
///
/// `C` is the wire codec, defaulting to [`JsonCodec`]. Register generated
/// `<Trait>MethodResponder`s with [`register`](Self::register), then call
/// [`into_router`](Self::into_router) / [`into_router_with_auth`](Self::into_router_with_auth).
pub struct ServiceHost<C = JsonCodec> {
    registry: Arc<Registry>,
    buffer: usize,
    _codec: PhantomData<C>,
}

impl<C> Default for ServiceHost<C> {
    fn default() -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            buffer: DEFAULT_BUFFER,
            _codec: PhantomData,
        }
    }
}

impl<C> ServiceHost<C> {
    /// Creates an empty host with a fresh `Registry` and the default buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adopts an already-populated `Registry` (e.g. one built by a `build_registry` helper).
    pub fn from_registry(registry: Arc<Registry>) -> Self {
        Self {
            registry,
            buffer: DEFAULT_BUFFER,
            _codec: PhantomData,
        }
    }

    /// Overrides the transport's inbound buffering capacity (default [`DEFAULT_BUFFER`]).
    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer;
        self
    }

    /// Returns a clone of the underlying `Registry` handle — an escape hatch for registration
    /// patterns the `register*` methods don't cover.
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Registers a generated responder for both unary and streaming dispatch (the usual case).
    pub fn register<M>(self, module: Arc<M>) -> Self
    where
        M: MethodExecutor + StreamingMethodExecutor + 'static,
    {
        self.registry.register(module);
        self
    }

    /// Registers a unary-only module executor.
    pub fn register_module(self, module: Arc<dyn MethodExecutor>) -> Self {
        self.registry.register_module(module);
        self
    }

    /// Registers a streaming-only module executor.
    pub fn register_streaming_module(self, module: Arc<dyn StreamingMethodExecutor>) -> Self {
        self.registry.register_streaming_module(module);
        self
    }
}

/// Gzip for responses that are complete when they are sent — never for the
/// streaming ones.
///
/// A streamed reply (`application/x-ndjson`) is a live channel: a big first
/// frame, then small deltas whenever something changes. Compressing it holds
/// those deltas until the encoder has enough input to emit a block — measured
/// at 300 ms, exactly as long as it took the NEXT frame to appear, which for a
/// monitoring dashboard means showing stale state. Unary replies have no such
/// tail, so they compress freely.
///
/// `SizeAbove` keeps the encoder off responses too small to gain from it.
fn compression_layer() -> tower_http::compression::CompressionLayer<
    tower_http::compression::predicate::And<
        tower_http::compression::predicate::NotForContentType,
        tower_http::compression::predicate::SizeAbove,
    >,
> {
    use tower_http::compression::{
        CompressionLayer,
        predicate::{NotForContentType, Predicate, SizeAbove},
    };
    CompressionLayer::new().compress_when(
        NotForContentType::const_new("application/x-ndjson").and(SizeAbove::new(1024)),
    )
}

impl<C: UnaryCodec + Send + Sync + 'static> ServiceHost<C> {
    /// Builds an **anonymous** (`POST /rpc`, no auth) router and spawns its dispatch loop.
    pub fn into_router(self) -> RpcService {
        let transport = AxumTransport::<C>::new(self.buffer);
        let router = transport.router().layer(compression_layer());
        let dispatch = tokio::spawn(serve_dispatch::<AxumTransport<C>, C>(
            transport,
            self.registry,
        ));
        RpcService { router, dispatch }
    }

    /// Builds an **authenticated** router (validates `Authorization: Bearer <token>` via
    /// `validator`) and spawns its dispatch loop. Any allex identity store is a
    /// [`BearerTokenValidator`].
    pub fn into_router_with_auth(self, validator: Arc<dyn BearerTokenValidator>) -> RpcService {
        let transport = AxumTransport::<C>::new(self.buffer);
        let router = transport
            .router_with_auth(validator)
            .layer(compression_layer());
        let dispatch = tokio::spawn(serve_dispatch::<AxumTransport<C>, C>(
            transport,
            self.registry,
        ));
        RpcService { router, dispatch }
    }
}

/// A wired RPC `axum::Router` plus the background dispatch task driving it.
///
/// Extend the router with [`map_router`](Self::map_router) / [`with_health`](Self::with_health),
/// then either [`serve`](Self::serve) it or take it apart with [`into_parts`](Self::into_parts) to
/// serve yourself.
pub struct RpcService {
    router: Router,
    dispatch: JoinHandle<()>,
}

impl RpcService {
    /// Splits into the raw `axum::Router` and the dispatch `JoinHandle` for full manual control.
    pub fn into_parts(self) -> (Router, JoinHandle<()>) {
        (self.router, self.dispatch)
    }

    /// Borrows the router (e.g. to inspect it) without consuming the service.
    pub fn router(&self) -> &Router {
        &self.router
    }

    /// Transforms the router in place — layer CORS, nest static files, add REST routes, etc.
    pub fn map_router(self, f: impl FnOnce(Router) -> Router) -> Self {
        Self {
            router: f(self.router),
            dispatch: self.dispatch,
        }
    }

    /// Adds a `GET /health` route returning `200 "ok"`.
    pub fn with_health(self) -> Self {
        self.map_router(|r| r.route("/health", axum::routing::get(|| async { "ok" })))
    }

    /// Serves the router on `listener`, racing the HTTP server against the dispatch loop: if
    /// either ends (server error, or the transport channel closes), this returns.
    pub async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        let Self {
            router,
            mut dispatch,
        } = self;
        tokio::select! {
            result = axum::serve(listener, router) => result,
            _ = &mut dispatch => Ok(()),
        }
    }
}

#[cfg(test)]
mod compression_tests {
    use super::compression_layer;
    use axum::{Router, http::header::CONTENT_TYPE, routing::get};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn response_headers_for(path: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Both bodies are well past the size floor, so only the content type
        // decides whether they compress.
        let body = "x".repeat(8192);
        let unary_body = body.clone();
        let stream_body = body.clone();
        let app = Router::new()
            .route(
                "/unary",
                get(|| async move { ([(CONTENT_TYPE, "application/json")], unary_body) }),
            )
            .route(
                "/stream",
                get(|| async move { ([(CONTENT_TYPE, "application/x-ndjson")], stream_body) }),
            )
            .layer(compression_layer());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut socket = TcpStream::connect(address).await.expect("connect");
        socket
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip\r\n\
                     Connection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("send");
        let mut raw = Vec::new();
        socket.read_to_end(&mut raw).await.expect("read");
        server.abort();
        String::from_utf8_lossy(&raw)
            .split("\r\n\r\n")
            .next()
            .unwrap_or_default()
            .to_lowercase()
    }

    #[tokio::test]
    async fn unary_replies_are_compressed() {
        let headers = response_headers_for("/unary").await;
        assert!(
            headers.contains("content-encoding: gzip"),
            "unary reply was not compressed:\n{headers}"
        );
    }

    /// The live stream must NOT be compressed: the encoder holds a frame until
    /// it has enough input to emit a block, which on a delta stream means the
    /// dashboard shows stale state until the next change happens to arrive.
    #[tokio::test]
    async fn streamed_replies_are_left_alone() {
        let headers = response_headers_for("/stream").await;
        assert!(
            !headers.contains("content-encoding: gzip"),
            "the streaming reply was compressed:\n{headers}"
        );
    }
}
