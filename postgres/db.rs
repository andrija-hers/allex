use std::future::Future;

use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

use crate::DbError;

/// A managed pool of PostgreSQL connections.
///
/// `Db` is `Clone`; clones share the underlying connection pool.
#[derive(Clone)]
pub struct Db {
    pool: Pool,
}

impl Db {
    /// Opens a pool against `conn_str` with up to 16 connections.
    pub fn connect(conn_str: &str) -> Result<Self, tokio_postgres::Error> {
        Self::connect_with_max(conn_str, 16)
    }

    /// Opens a pool against `conn_str` with `max_connections` connections.
    pub fn connect_with_max(
        conn_str: &str,
        max_connections: usize,
    ) -> Result<Self, tokio_postgres::Error> {
        let cfg: Config = conn_str.parse()?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = deadpool_postgres::Manager::from_config(cfg, NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(max_connections)
            .build()
            .expect("pool config is valid");
        Ok(Db { pool })
    }

    /// Checks out a single client from the pool.
    pub async fn get_client(
        &self,
    ) -> Result<deadpool::managed::Object<deadpool_postgres::Manager>, DbError> {
        self.pool.get().await.map_err(DbError::from)
    }

    /// Runs `f` with a pooled client, mapping pool acquisition errors into `E`.
    ///
    /// This is the primary entry point for batch operations: check out one
    /// client and issue multiple statements before returning, completing the
    /// full interaction in a single server roundtrip sequence.
    ///
    /// ```ignore
    /// let result = db.with_client(|client| async move {
    ///     client.execute("INSERT INTO ...", &[...]).await?;
    ///     let rows = client.query("SELECT ...", &[...]).await?;
    ///     Ok(rows)
    /// }).await?;
    /// ```
    pub async fn with_client<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(deadpool::managed::Object<deadpool_postgres::Manager>) -> Fut + Send,
        Fut: Future<Output = Result<R, E>> + Send,
        R: Send,
        E: From<deadpool_postgres::PoolError> + Send,
    {
        let client = self.pool.get().await.map_err(E::from)?;
        f(client).await
    }
}
