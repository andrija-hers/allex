# allex-transport

> Wire types, codecs, and transports for allex RPC.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

The wire layer: `Payload`, `Error`, `ModuleMethodCall`, the `JsonCodec`, and
`AxumTransport` (`router` / `router_with_auth`), plus reqwest, WebSocket, and
SignalR transports. It also defines `BearerTokenValidator`, the seam an identity
store implements to authenticate callers.

Payloads are opaque bytes that are, with a JSON codec, always UTF-8 text — so a
human-readable format carries them as a **JSON string**, not as an array of byte
values. That changed in 2026-07 and is a **wire** break for peers built earlier
(both forms are still read, only the string form is written). Before upgrading a
project whose clients are already installed, read "Upgrading allex where clients
are already in the field" in `AGENTS.md`.

Key ideas: payloads are **opaque bytes**, the codec is a *type* not a value, and
`Error::App` is the seam between a transport fault and an application error. This
crate is where the **error → HTTP status** table is enforced: application error →
422, failed role check → 403, bad body → 400, transport fault → 500. There is no
401.

## Docs & license

API documentation: **[docs.rs/allex-transport](https://docs.rs/allex-transport)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
