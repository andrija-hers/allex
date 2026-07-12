//! **SQLite-backed** `IdentityV1` + `BearerTokenValidator` — one of the interchangeable stock
//! identity stores.
//!
//! Same model as every stock store: bcrypt over `users(handle, password_hash, role)` plus a durable
//! `refresh_tokens` table. `new(path, secret)` opens a single database file and creates the tables
//! if absent — zero-config, embedded, no separate server process. Reach for it on single-node
//! deployments and for tests that need to outlive `InMemoryIdentityStore`. Pick a store by
//! deployment; the decision guide (stock store vs `CredentialStore`) lives in `allex-identity`.

mod store;
pub use store::*;
