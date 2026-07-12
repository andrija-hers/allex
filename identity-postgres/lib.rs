//! **PostgreSQL-backed** `IdentityV1` + `BearerTokenValidator` — one of the interchangeable
//! stock identity stores.
//!
//! Same model as every stock store: bcrypt over `users(handle, password_hash, role)` plus a durable
//! `refresh_tokens` table. `new(db, secret)` takes an `allex_postgres::Db` pool and creates its
//! tables if absent (it also owns a `message_templates` table). Reach for it on real multi-process
//! deployments where identity must live in a shared database. Pick a store by deployment; the
//! decision guide (stock store vs `CredentialStore`) lives in `allex-identity`.

mod store;
pub use store::*;
