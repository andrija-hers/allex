use deadpool_postgres::PoolError;

/// Error type returned by pool and query operations.
///
/// The optional `AppError` type parameter lets closures passed to
/// [`crate::Db::with_client`] return domain errors alongside database errors, all
/// through a single `?`-compatible type. Use the default (`Infallible`) when
/// no application-level errors are needed.
#[derive(Debug, thiserror::Error)]
pub enum DbError<AppError: std::fmt::Debug + std::fmt::Display = std::convert::Infallible> {
    #[error("pool error: {0}")]
    Pool(String),

    #[error("postgres error: {0}")]
    Postgres(String),

    #[error("app error: {0}")]
    App(AppError),
}

impl<E: std::fmt::Debug + std::fmt::Display> From<PoolError> for DbError<E> {
    fn from(e: PoolError) -> Self {
        DbError::Pool(e.to_string())
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> From<tokio_postgres::Error> for DbError<E> {
    fn from(e: tokio_postgres::Error) -> Self {
        DbError::Postgres(e.to_string())
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> DbError<E> {
    /// Wraps an application-level error.
    pub fn app(e: impl Into<E>) -> Self {
        DbError::App(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_constructor_wraps_and_displays() {
        let e: DbError<String> = DbError::app("something failed");
        assert!(matches!(e, DbError::App(_)));
        assert!(e.to_string().contains("something failed"));
    }

    #[test]
    fn pool_variant_display_contains_prefix() {
        let e: DbError<String> = DbError::Pool("timed out".into());
        assert!(e.to_string().contains("pool error"));
        assert!(e.to_string().contains("timed out"));
    }

    #[test]
    fn postgres_variant_display_contains_prefix() {
        let e: DbError<String> = DbError::Postgres("syntax error at position 0".into());
        assert!(e.to_string().contains("postgres error"));
        assert!(e.to_string().contains("syntax error"));
    }

    #[test]
    fn app_variant_is_not_pool_or_postgres() {
        let e: DbError<String> = DbError::app("domain problem".to_string());
        assert!(!matches!(e, DbError::Pool(_)));
        assert!(!matches!(e, DbError::Postgres(_)));
    }
}
