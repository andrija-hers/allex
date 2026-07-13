# allex-transport

> Wire types, codecs, and transports for allex RPC.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

The wire layer: `Payload`, `Error`, `ModuleMethodCall`, the `JsonCodec`, and
`AxumTransport` (`router` / `router_with_auth`), plus reqwest, WebSocket, and
SignalR transports. It also defines `BearerTokenValidator`, the seam an identity
store implements to authenticate callers.

Key ideas: payloads are **opaque bytes**, the codec is a *type* not a value, and
`Error::App` is the seam between a transport fault and an application error. This
crate is where the **error → HTTP status** table is enforced: application error →
422, failed role check → 403, bad body → 400, transport fault → 500. There is no
401.

## Docs & license

API documentation: **[docs.rs/allex-transport](https://docs.rs/allex-transport)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
