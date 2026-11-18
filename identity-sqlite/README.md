# allex-identity-sqlite

> SQLite-backed identity store for allex.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A SQLite-backed `IdentityV1` store. Stores `users(handle, password_hash, role)`
with **bcrypt** hashing, mints and rotates JWT sessions, and implements both
`BearerTokenValidator` (so it plugs straight into `router_with_auth`) and
`AccountStore` (password change, session issue/revoke).

A good fit for single-node deployments and local development where an embedded
database beats running a server.

## Docs & license

API documentation: **[docs.rs/allex-identity-sqlite](https://docs.rs/allex-identity-sqlite)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
