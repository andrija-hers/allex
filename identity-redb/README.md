# allex-identity-redb

> redb-backed identity store for allex.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A [redb](https://crates.io/crates/redb)-backed `IdentityV1` store: a pure-Rust
embedded key-value store, no SQL engine. Same contract as the other stores —
bcrypt credentials, JWT sessions, `BearerTokenValidator` + `AccountStore`.

A good fit when you want an embedded store without linking SQLite.

## Docs & license

API documentation: **[docs.rs/allex-identity-redb](https://docs.rs/allex-identity-redb)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
