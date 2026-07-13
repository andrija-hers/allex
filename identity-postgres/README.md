# allex-identity-postgres

> Postgres-backed identity store for allex.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A Postgres-backed `IdentityV1` store (via `allex-postgres`'s pooled access). Same
contract as the other stores — bcrypt over `users(handle, password_hash, role)`,
JWT sessions, `BearerTokenValidator` + `AccountStore` — backed by a shared
Postgres server.

The integration tests use testcontainers and require Docker.

## Docs & license

API documentation: **[docs.rs/allex-identity-postgres](https://docs.rs/allex-identity-postgres)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
