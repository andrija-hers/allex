# allex-postgres-macros

> The FromRow derive macro behind allex-postgres.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Generates the boilerplate to build a struct from a `tokio_postgres::Row`,
resolving column names and types against the re-exports in `allex-postgres`.

You normally depend on `allex-postgres` (which re-exports this), not on this crate
directly.

## Docs & license

API documentation: **[docs.rs/allex-postgres-macros](https://docs.rs/allex-postgres-macros)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
