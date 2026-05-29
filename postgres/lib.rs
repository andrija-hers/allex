//! Shared PostgreSQL infrastructure for allex.
//!
//! Provides a connection pool wrapper ([`Db`]), a generic error type ([`DbError`]),
//! row extension traits ([`RowExt`], [`RowVecExt`]), and the [`FromRow`] derive macro
//! re-exported for convenience.

pub use allex_postgres_macros::FromRow;

// Re-export the underlying crates so macro-generated code and downstream
// crates do not need to add them as direct dependencies.
pub use deadpool_postgres;
pub use tokio_postgres;

mod error;
pub use error::*;

mod db;
pub use db::*;

mod row_ext;
pub use row_ext::*;
