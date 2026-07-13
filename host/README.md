# allex-host

> Batteries-included axum host builder — start here for a new backend.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

`ServiceHost` owns the `Registry` + `AxumTransport` + `serve_dispatch` wiring
(including the one spawn-order footgun) and hands you back a plain `axum::Router`,
so you still add your own CORS, `/health`, static files, or REST routes before
serving.

The `bootstrap` module owns the startup prologue every backend used to
copy-paste: JWT-secret load-or-create, TTL/env parsing, `.env` loading, and
bind-address resolution.

## Example

```rust
use allex_host::ServiceHost;
use allex_transport::JsonCodec;

let (router, _dispatch) = ServiceHost::<JsonCodec>::new()
    .register(Arc::new(IdentityV1MethodResponder::<_, JsonCodec>::new(identity)))
    .register(Arc::new(CounterV1MethodResponder::<_, JsonCodec>::new(counter)))
    .into_router_with_auth(validator)   // any identity store is a validator
    .into_parts();                      // -> (axum::Router, JoinHandle)
```

## Docs & license

API documentation: **[docs.rs/allex-host](https://docs.rs/allex-host)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
