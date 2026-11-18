//! # allex
//!
//! Umbrella crate for the **allex** framework — a contract-first RPC framework for
//! Rust. Declare a service as a `#[rpc_module]` trait and a proc-macro generates
//! the client, the server glue, and the authorization checks; separate codegen
//! emits byte-compatible Dart and TypeScript clients; and runtime crates handle
//! transport, dispatch, identity, streaming, and state.
//!
//! This crate re-exports the individual `allex-*` crates under short module names,
//! gated by features, so you can depend on the framework with a single line and
//! enable only what you use:
//!
//! ```toml
//! [dependencies]
//! allex = "0.1"                         # default = ["host", "identity"]
//! # or pick features explicitly:
//! # allex = { version = "0.1", default-features = false, features = ["contract", "codegen"] }
//! ```
//!
//! With the default features you get [`host`], [`serve`], the contract stack
//! ([`transport`], [`execution`], [`progressable`], and the `rpc_module` macro),
//! and [`identity`]. See the crate's `Cargo.toml` for the full feature list, and
//! the [repository](https://github.com/andrija-hers/allex) for the full README,
//! `AGENTS.md` (the orientation guide), and `COMPARISON.md`.
//!
//! Each module below is the corresponding `allex-*` crate; consult its own docs
//! for detail.

// The `#[rpc_module]` attribute macro, re-exported at the crate root so
// `use allex::rpc_module;` works. `#[public]` and `#[allowed_roles(..)]` are inner
// helper attributes consumed by `#[rpc_module]` — they are not imported.
#[cfg(feature = "allex-rpc-macros")]
pub use allex_rpc_macros::rpc_module;

#[cfg(feature = "allex-execution")]
pub use allex_execution as execution;
#[cfg(feature = "allex-host")]
pub use allex_host as host;
#[cfg(feature = "allex-serve")]
pub use allex_serve as serve;
#[cfg(feature = "allex-transport")]
pub use allex_transport as transport;
#[cfg(feature = "allex-progressable-future")]
pub use progressable_future as progressable;

#[cfg(feature = "allex-identity")]
pub use allex_identity as identity;
#[cfg(feature = "allex-identity-postgres")]
pub use allex_identity_postgres as identity_postgres;
#[cfg(feature = "allex-identity-redb")]
pub use allex_identity_redb as identity_redb;
#[cfg(feature = "allex-identity-sqlite")]
pub use allex_identity_sqlite as identity_sqlite;

#[cfg(feature = "allex-keyed-events")]
pub use allex_keyed_events as keyed_events;
#[cfg(feature = "allex-scheduler")]
pub use allex_scheduler as scheduler;
#[cfg(feature = "allex-state")]
pub use allex_state as state;

#[cfg(feature = "allex-dart-codegen")]
pub use allex_dart_codegen as dart_codegen;
#[cfg(feature = "allex-ts-codegen")]
pub use allex_ts_codegen as ts_codegen;

#[cfg(feature = "allex-awaitable-map")]
pub use allex_awaitable_map as awaitable_map;
#[cfg(feature = "allex-guards")]
pub use allex_guards as guards;
#[cfg(feature = "allex-mailgunner")]
pub use allex_mailgunner as mailgunner;
#[cfg(feature = "allex-message-templates")]
pub use allex_message_templates as message_templates;
#[cfg(feature = "allex-postgres")]
pub use allex_postgres as postgres;
#[cfg(feature = "allex-script")]
pub use allex_script as script;
#[cfg(feature = "allex-watch-singleton")]
pub use allex_watch_singleton as watch_singleton;
#[cfg(feature = "allex-winsvc")]
pub use allex_winsvc as winsvc;
