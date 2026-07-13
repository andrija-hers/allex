# allex-serve

> Transport-agnostic dispatch loop and the ambient caller context.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Drives requests off a transport and into the registry: `serve_dispatch` drains a
transport and spawns a task per request. Deliberately **transport-agnostic** — it
never mentions axum.

It also installs the **caller context** as a task-local, so a method reads *who is
calling* without taking a caller argument: `caller_handle()`, `caller_role()`,
`caller_profile()`. For per-object authorization inside a unary method ("only the
creator may edit this"), use `deny_access()` / `authorize(cond, err)?` so the
denial becomes a **403**, not a domain 422.

## Example

```rust
// inside an #[rpc_module] method implementation
let who = allex_serve::caller_handle()?;                          // authenticated handle
allex_serve::authorize(record.owner == who, MyError::NotOwner)?;  // -> 403 on failure
```

## Docs & license

API documentation: **[docs.rs/allex-serve](https://docs.rs/allex-serve)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
