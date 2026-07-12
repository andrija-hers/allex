//! **redb-backed** `IdentityV1` + `BearerTokenValidator` — one of the interchangeable stock
//! identity stores.
//!
//! Same model as every stock store: bcrypt over `users(handle, password_hash, role)` plus durable
//! refresh-token state. `new(path, ..)` opens (or creates) an embedded [redb](https://docs.rs/redb)
//! key-value database file — pure Rust, no SQL and no C dependency. Reach for it on single-node
//! deployments that want an embedded store without linking SQLite. Pick a store by deployment; the
//! decision guide (stock store vs `CredentialStore`) lives in `allex-identity`.
//!
//! # Memory footprint is tunable — don't judge redb by its defaults
//!
//! This store opens the database with `Database::open`/`create`, i.e. **redb's default cache
//! size**, which can look RAM-hungry out of the box. redb's memory use is configurable: build the
//! database via `redb::Builder::set_cache_size(bytes)` before `create`/`open` to cap it. If redb's
//! footprint is ever a concern here, thread a builder (and a cache-size argument) through `new`
//! rather than concluding the backend is unsuitable — the default cache, not redb itself, is the
//! knob.

mod store;

pub use store::*;
