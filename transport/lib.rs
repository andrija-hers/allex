//! The wire vocabulary: **opaque payloads, a codec chosen by type, one error enum, and the
//! error→HTTP-status mapping that the rest of the workspace treats as canonical.**
//!
//! This crate defines *what goes on the wire* and ships the concrete transports that move it. It
//! knows nothing about your service traits — `#[rpc_module]` (in `allex-rpc-macros`) generates code
//! that speaks this vocabulary.
//!
//! # Everything on the wire is an opaque [`Payload`]; the codec is a *type*, not a value
//!
//! A [`Payload`] is just `Vec<u8>` — already-serialized bytes. Nothing here inspects them. The
//! codec is a **type parameter** `C: UnaryCodec` threaded through every API; its `encode`/`decode`
//! are associated functions, so you pass `JsonCodec` as a type argument, never an instance.
//! [`JsonCodec`] (serde_json, `application/json`) is the only codec shipped. Any `Serialize` value
//! gets `.encode::<C>()` for free via the [`Encodable`] blanket impl, and [`Payload::decode`] turns
//! bytes back into a `T`.
//!
//! Calls are addressed by a **nesting hierarchy of tuples**, each layer adding a routing prefix
//! around a pre-encoded payload:
//!
//! - [`MethodCall`] `(method, payload)` — method-addressed only.
//! - [`ModuleMethodCall`] `(module, MethodCall)` — the normal unit: module + method. (Remember the
//!   module name is the `foo_v_1`-style snake_case the macro derives.)
//! - [`RelayedModuleMethodCall`] `(server, ModuleMethodCall)` — relayed through a named upstream.
//!
//! A streaming reply is a sequence of [`CallResponse`] frames — `Progress(_)` (non-terminal), then
//! one terminal `Success(_)` or `Error(_)`. For a "billboard" stream that never returns normally,
//! use [`Never`] as the success type (`ProgressableFuture<P, Never, E>`).
//!
//! # One [`Error`], three outcomes — the transport-fault / app-error / success split
//!
//! [`Error`] is the *transport* error enum, but its most important variant is **[`Error::App`],
//! which carries an application-level error as an opaque [`Payload`]**. That is the seam between "a
//! wire/plumbing fault" and "your domain returned `Err(YourError)`": the two are never confused
//! because the app error rides inside `Error::App`, still codec-encoded. On the client this widens
//! into a three-way [`ProgressOutcome`] (re-exported here): `Success` / `Error` (your decoded app
//! error) / `Transport` (a genuine fault). [`Error::surely_encode`] is the infallible encoder used
//! on the error path — it can never itself fail the response.
//!
//! # Error → HTTP status lives *here* (the axum transport) and is the source of truth
//!
//! The workspace `AGENTS.md` quotes this table; **this crate is where it is actually enforced**
//! (`transport/transport/axum.rs`), and the reqwest client applies the exact inverse so the mapping
//! is symmetric across the HTTP boundary:
//!
//! | Result | HTTP |
//! |---|---|
//! | unary success | **200** |
//! | streaming success | **200**, `Content-Type: application/x-ndjson` |
//! | application error (`Err(YourError)` → `Error::App`) | **422** |
//! | failed role check (`Error::AccessForbidden`) | **403** |
//! | unregistered module (`Error::ModuleNotFound`) | **404** |
//! | undecodable request body | **400** |
//! | dispatch/channel fault | **500** |
//!
//! Two deliberate choices: **there is no 401** (403 is authz, 422 is a domain/credential error),
//! and the 422 body is the **raw encoded app-error bytes**, not the transport's byte-array wrapper,
//! so a non-Rust HTTP client sees readable JSON.
//!
//! # The authentication seam: [`BearerTokenValidator`]
//!
//! `AxumTransport::router_with_auth(validator)` pulls the `Authorization: Bearer <token>` header and
//! calls [`BearerTokenValidator::validate_bearer`], which returns `Option<String>` — the caller
//! identity **wire string** (`handle\x1erole[\x1eprofile]`, RS = 0x1E) on success, `None` for
//! anonymous. That string flows to dispatch as `caller_role`; the generated permission check and
//! `allex-serve` split it back into handle/role/profile. `router()` is the anonymous variant. An
//! identity store (the `allex-identity` crate) is what implements `BearerTokenValidator`.
//!
//! # Transports shipped, and the client/server trait split
//!
//! Concrete transports: `AxumTransport` (HTTP server), reqwest (HTTP client), websocket and signalr
//! (both directions), and an in-process serial transport for tests. They implement one side of a
//! trait pair — `ClientSideOneShotTransport` / `ClientSideChunkedTransport` (what a generated
//! **client** is bounded on) versus `ServerSide*Transport` + `*Item` (what feeds server dispatch).
//! That split is why the macro's client blanket-impls "for any transport `T: ClientSide…`".
//!
//! ```
//! use allex_transport::{JsonCodec, ModuleMethodCall};
//!
//! // The codec is a type argument; payloads are opaque bytes addressed by module+method.
//! let call = ModuleMethodCall::new::<JsonCodec, _>("identity_v_1", "login", ("alice", "s3cret")).unwrap();
//! assert_eq!(call.get_module(), "identity_v_1");
//! assert_eq!(call.get_method(), "login");
//!
//! // Decode the payload back with the same codec.
//! let (user, _pw): (String, String) = call.get_payload().decode::<JsonCodec, _>().unwrap();
//! assert_eq!(user, "alice");
//! ```

// The client/server transports implement trait methods whose signatures are `-> impl Future + Send`
// (so they can't be written as `async fn`), and use some deliberately-nested `Arc<Mutex<HashMap<..>>>`
// pending-call maps — both are intentional; silence the corresponding style lints crate-wide.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

pub use progressable_future::{ProgressOutcome, TransportIssue};
pub use tracing;
#[doc(hidden)]
pub mod rpc_trace;
mod transport_types;
pub use transport_types::*;
mod transport;
pub use transport::*;
mod codecs;
pub use codecs::*;
