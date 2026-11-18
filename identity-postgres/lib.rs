//! **PostgreSQL-backed** `IdentityV1` + `BearerTokenValidator` — one of the interchangeable
//! stock identity stores.
//!
//! Same model as every stock store: bcrypt over `users(handle, password_hash, role)` plus a durable
//! `refresh_tokens` table. `new(db, secret)` takes an `allex_postgres::Db` pool and creates its
//! tables if absent (it also owns a `message_templates` table). Reach for it on real multi-process
//! deployments where identity must live in a shared database. Pick a store by deployment; the
//! decision guide (stock store vs `CredentialStore`) lives in `allex-identity`.
//!
//! **Refresh tokens are stored only as keys** — the SHA-256 of the token, see
//! [`refresh_token_key`]. A dump of the table cannot be replayed as sessions. A host that
//! reads `refresh_tokens` itself must look tokens up through `refresh_token_key`, never
//! by the token. `setup()` converts the rows of an older deployment in place, so
//! upgrading signs nobody out. **The upgrade is one-way:** an older build can
//! no longer find any token, so rolling it back signs every user out once (nothing else
//! breaks — they sign in again).

mod store;
pub use store::*;
