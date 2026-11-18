# allex-winsvc

> Thin Windows-service host harness for allex backends.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Wrap your async run-loop so it installs and behaves as a proper Windows service.

Windows-only: on every other platform the crate compiles to nothing, so it can
sit in a cross-platform workspace and be depended on unconditionally.

## Docs & license

API documentation: **[docs.rs/allex-winsvc](https://docs.rs/allex-winsvc)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
