# allex-postgres

> Pooled Postgres access with a FromRow derive.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

`Db` wraps a `deadpool_postgres` pool; run queries with
`with_client(|client| async { … })`. Ships a `FromRow` derive (from
`allex-postgres-macros`) for mapping rows to structs.

It **re-exports** `tokio_postgres` and `deadpool_postgres`, so dependents — and
the code `FromRow` generates — can name those types without adding direct
dependencies. The integration tests use testcontainers and require Docker.

## Docs & license

API documentation: **[docs.rs/allex-postgres](https://docs.rs/allex-postgres)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
