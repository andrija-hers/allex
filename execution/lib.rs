//! The **dispatch seam** — routes a decoded call to the right module, and is where the
//! app-error-vs-transport-error split (that becomes HTTP 422 vs 403) is actually decided.
//!
//! This crate sits between `transport` (bytes + module/method *names*) and the generated
//! `<Trait>MethodResponder`s (typed handlers). It does no serialization of its own; it routes by the
//! wire-visible module-name string and hands the still-encoded `transport::Payload` onward. You
//! rarely call any of it directly — the `#[rpc_module]` macro implements these traits and
//! `allex-serve` drives them; **your one touchpoint is building a [`Registry`] and `register`-ing
//! responders into it.**
//!
//! # [`Registry`]: one `register` wires both unary *and* streaming
//!
//! [`Registry`] is a `DashMap`-backed router holding three tables — unary modules, streaming
//! modules, and relayed upstream servers — each keyed by the `foo_v_1`-style wire module name. A
//! generated responder implements *both* [`MethodExecutor`] and [`StreamingMethodExecutor`], so
//! [`Registry::register`] inserts it into the unary and streaming tables in one call — that's the
//! normal entry point (the `register_module` / `register_streaming_module` / `register_server`
//! variants exist for the rare split case). `Registry` itself implements [`ModuleExecutor`] and
//! [`RelayedModuleExecutor`], so the whole server routes as a single object.
//!
//! # The trait tiers mirror the wire addressing
//!
//! - [`MethodExecutor`] — dispatch *within* one module (method → payload).
//! - [`ModuleExecutor`] — dispatch *across* modules (module + method). `Registry` is one.
//! - [`RelayedModuleExecutor`] — dispatch to another *named server*. `Registry` is one too.
//! - [`StreamingMethodExecutor`] — the streaming counterpart, dispatched **separately**:
//!   [`Registry::execute_streaming`] returns `None` when the target isn't a streaming endpoint, and
//!   the caller falls back to unary. Unknown module → `Error::ModuleNotFound`; unknown relayed
//!   server → `Error::ServerNotFound`.
//!
//! # Where 422-vs-403 is born: [`Response`] → `Result`
//!
//! Generated responders return the rich [`Response`] enum (`Success` / `Progress` / `Error` /
//! `TransportError`). The blanket `MethodExecutor` impl flattens it into a `transport::Result`:
//!
//! - `Success` / `Progress` → `Ok(payload)`
//! - `Error(payload)` → `Err(Error::App(payload))` — a **domain** error (→ HTTP 422)
//! - `TransportError(err)` → `Err(err)` — passed through **without** app-error wrapping, so an auth
//!   rejection (`AccessForbidden`) stays a transport fault (→ HTTP 403)
//!
//! That distinction is the whole reason [`MethodResponder`] returns `Response` instead of a bare
//! `Result`: it keeps "you were forbidden" from being mis-encoded as "your call returned an error".
//! `caller_role` threads through every tier untouched — authorization is enforced inside the
//! responder (macro-generated), never here.
//!
//! ```
//! use allex_execution::Registry;
//! use transport::Payload;
//!
//! // Construction and streaming lookup are synchronous; an empty registry has no streaming endpoint.
//! let registry = Registry::new();
//! assert!(registry.execute_streaming("foo_v_1", "bar", Payload::new(b""), None).is_none());
//! ```

mod execution_types;
pub use execution_types::*;
mod registry;
pub use registry::*;
mod response;
pub use response::*;
