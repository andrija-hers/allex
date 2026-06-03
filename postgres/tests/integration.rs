//! Integration tests for `allex-postgres`.
//!
//! Each test spins up a fresh Postgres container via `testcontainers`.
//! **Requires a running Docker daemon.**  Run with:
//!
//! ```text
//! cargo test -p allex-postgres
//! ```

use allex_postgres::{Db, DbError, FromRow, RowExt, RowVecExt};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Starts a Postgres container and returns a connected [`Db`].
///
/// The container is returned so its lifetime is tied to the test function;
/// dropping it stops the container.
async fn start_db() -> (Db, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("Docker must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let db = Db::connect(&conn_str).expect("connection string must be valid");
    (db, container)
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn simple_query_returns_correct_value() {
    let (db, _c) = start_db().await;
    let result: i32 = db
        .with_client(|client| async move {
            let rows = client.query("SELECT 42::int4 AS n", &[]).await?;
            let val: i32 = rows[0].get("n");
            Ok::<_, DbError>(val)
        })
        .await
        .unwrap();
    assert_eq!(result, 42);
}

// ---------------------------------------------------------------------------
// Batch: multiple statements in one connection checkout
// ---------------------------------------------------------------------------

/// The key use-case from the original `pg-helper`: execute several statements
/// against the same checked-out client without returning it to the pool between
/// operations.
#[tokio::test]
async fn batch_multiple_statements_single_connection_checkout() {
    let (db, _c) = start_db().await;

    let rows = db
        .with_client(|client| async move {
            client
                .execute(
                    "CREATE TABLE IF NOT EXISTS batch_test (id SERIAL PRIMARY KEY, val TEXT NOT NULL)",
                    &[],
                )
                .await?;
            client
                .execute("INSERT INTO batch_test (val) VALUES ($1)", &[&"hello"])
                .await?;
            client
                .execute("INSERT INTO batch_test (val) VALUES ($1)", &[&"world"])
                .await?;
            let rows = client.query("SELECT val FROM batch_test ORDER BY id", &[]).await?;
            Ok::<_, DbError>(rows)
        })
        .await
        .unwrap();

    let vals: Vec<String> = rows.iter().map(|r| r.get::<_, String>("val")).collect();
    assert_eq!(vals, ["hello", "world"]);
}

// ---------------------------------------------------------------------------
// RowExt
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row_ext_get_or_returns_value_when_column_present() {
    let (db, _c) = start_db().await;
    let val = db
        .with_client(|client| async move {
            let rows = client.query("SELECT 'hi'::text AS msg", &[]).await?;
            let s: String = rows[0].get_or("msg", "default".to_string());
            Ok::<_, DbError>(s)
        })
        .await
        .unwrap();
    assert_eq!(val, "hi");
}

#[tokio::test]
async fn row_ext_get_or_returns_default_for_missing_column() {
    let (db, _c) = start_db().await;
    let val = db
        .with_client(|client| async move {
            let rows = client.query("SELECT 1::int4 AS other", &[]).await?;
            // "nonexistent" column does not exist — get_or must fall back to default
            let s: String = rows[0].get_or("nonexistent", "fallback".to_string());
            Ok::<_, DbError>(s)
        })
        .await
        .unwrap();
    assert_eq!(val, "fallback");
}

// ---------------------------------------------------------------------------
// RowVecExt
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row_vec_ext_get_or_returns_value_at_valid_index() {
    let (db, _c) = start_db().await;
    let val = db
        .with_client(|client| async move {
            let rows = client
                .query("SELECT n FROM generate_series(10, 12) AS n", &[])
                .await?;
            // row 1 (0-indexed), column "n"
            let v: i32 = rows.get_or(1, "n", -1_i32);
            Ok::<_, DbError>(v)
        })
        .await
        .unwrap();
    assert_eq!(val, 11);
}

#[tokio::test]
async fn row_vec_ext_get_or_returns_default_for_out_of_range_row() {
    let (db, _c) = start_db().await;
    let val = db
        .with_client(|client| async move {
            let rows = client.query("SELECT 1::int4 AS n", &[]).await?;
            // row 99 does not exist
            let v: i32 = rows.get_or(99, "n", -999_i32);
            Ok::<_, DbError>(v)
        })
        .await
        .unwrap();
    assert_eq!(val, -999);
}

// ---------------------------------------------------------------------------
// FromRow derive macro
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, FromRow)]
struct Person {
    name: String,
    age: i32,
}

#[derive(Debug, PartialEq, FromRow)]
struct Aliased {
    #[row(rename = "full_name")]
    name: String,
    score: i32,
}

#[tokio::test]
async fn from_row_derive_maps_fields_by_column_name() {
    let (db, _c) = start_db().await;
    let person = db
        .with_client(|client| async move {
            let rows = client
                .query("SELECT 'Alice'::text AS name, 30::int4 AS age", &[])
                .await?;
            let p = Person::try_from(rows.into_iter().next().unwrap())?;
            Ok::<_, DbError>(p)
        })
        .await
        .unwrap();
    assert_eq!(
        person,
        Person {
            name: "Alice".into(),
            age: 30
        }
    );
}

#[tokio::test]
async fn from_row_derive_rename_attribute_reads_aliased_column() {
    let (db, _c) = start_db().await;
    let record = db
        .with_client(|client| async move {
            let rows = client
                .query("SELECT 'Bob'::text AS full_name, 95::int4 AS score", &[])
                .await?;
            let a = Aliased::try_from(rows.into_iter().next().unwrap())?;
            Ok::<_, DbError>(a)
        })
        .await
        .unwrap();
    assert_eq!(
        record,
        Aliased {
            name: "Bob".into(),
            score: 95
        }
    );
}

// ---------------------------------------------------------------------------
// Pool sizing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connect_with_max_one_allows_sequential_queries() {
    let (_, c) = start_db().await;
    let host = c.get_host().await.unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let db = Db::connect_with_max(&conn_str, 1).expect("valid");

    for i in 0_i32..3 {
        let result = db
            .with_client(|client| async move {
                let rows = client.query("SELECT $1::int4 AS n", &[&i]).await?;
                let v: i32 = rows[0].get("n");
                Ok::<_, DbError>(v)
            })
            .await
            .unwrap();
        assert_eq!(result, i);
    }
}
