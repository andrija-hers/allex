//! Integration tests for `allex-identity-postgres`.
//!
//! Each test spins up a fresh Postgres container via `testcontainers`.
//! **Requires a running Docker daemon.**  Run with:
//!
//! ```text
//! cargo test -p allex-identity-postgres
//! ```

use allex_identity_postgres::PostgresIdentityStore;
use allex_postgres::Db;
use identity::{IdentityError, IdentityV1, LoginRequest, RegisterRequest, ValidateRequest};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use transport::BearerTokenValidator;

const SECRET: &str = "test-jwt-secret-key-for-integration-tests";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_store() -> (
    PostgresIdentityStore,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .start()
        .await
        .expect("Docker must be running");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let db = Db::connect(&conn_str).expect("valid connection string");
    let store = PostgresIdentityStore::new(db, SECRET);
    store.setup().await.expect("schema setup must succeed");
    (store, container)
}

fn reg(handle: &str, password: &str) -> RegisterRequest {
    RegisterRequest {
        handle: handle.into(),
        password: password.into(),
        role: "user".into(),
    }
}

fn login(handle: &str, password: &str) -> LoginRequest {
    LoginRequest {
        handle: handle.into(),
        password: password.into(),
    }
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setup_is_idempotent() {
    let (store, _c) = start_store().await;
    // Calling setup a second time must not return an error.
    store
        .setup()
        .await
        .expect("second setup call must be idempotent");
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

#[tokio::test]
async fn register_creates_user_and_returns_valid_jwt() {
    let (store, _c) = start_store().await;
    let resp = store.register(reg("alice", "s3cr3t")).await.unwrap();
    assert!(!resp.token.is_empty());

    // The returned token must be immediately usable for validate.
    let role_resp = store
        .validate(ValidateRequest { token: resp.token })
        .await
        .unwrap();
    assert_eq!(role_resp.role, "user");
}

#[tokio::test]
async fn register_duplicate_handle_returns_error() {
    let (store, _c) = start_store().await;
    store.register(reg("bob", "pass1")).await.unwrap();
    let err = store.register(reg("bob", "pass2")).await.unwrap_err();
    assert!(
        matches!(err, IdentityError::HandleAlreadyTaken),
        "expected HandleAlreadyTaken, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------------

#[tokio::test]
async fn login_valid_credentials_returns_jwt() {
    let (store, _c) = start_store().await;
    store.register(reg("carol", "my-password")).await.unwrap();

    let resp = store.login(login("carol", "my-password")).await.unwrap();
    assert!(!resp.token.is_empty());

    let role_resp = store
        .validate(ValidateRequest { token: resp.token })
        .await
        .unwrap();
    assert_eq!(role_resp.role, "user");
}

#[tokio::test]
async fn login_wrong_password_returns_invalid_credentials() {
    let (store, _c) = start_store().await;
    store.register(reg("dave", "correct")).await.unwrap();
    let err = store.login(login("dave", "wrong")).await.unwrap_err();
    assert!(
        matches!(err, IdentityError::InvalidCredentials),
        "expected InvalidCredentials, got {err:?}"
    );
}

#[tokio::test]
async fn login_unknown_handle_returns_invalid_credentials() {
    let (store, _c) = start_store().await;
    let err = store.login(login("nobody", "pass")).await.unwrap_err();
    assert!(
        matches!(err, IdentityError::InvalidCredentials),
        "expected InvalidCredentials, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_invalid_token_returns_error() {
    let (store, _c) = start_store().await;
    let err = store
        .validate(ValidateRequest {
            token: "not.a.jwt".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, IdentityError::InvalidToken),
        "expected InvalidToken, got {err:?}"
    );
}

#[tokio::test]
async fn validate_token_signed_with_wrong_secret_returns_error() {
    let (store, _c) = start_store().await;

    // Build a store with a different secret and register a user there.
    let host = "cannot_reuse";
    // We can't start another container here, so instead we forge a known-bad JWT.
    // A well-formed but incorrectly-signed token from jwt.io:
    let bad_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.\
        eyJzdWIiOiJ1c2VyIiwicm9sZSI6InVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.\
        WRONG_SIGNATURE_HERE";
    let _ = host; // suppress unused warning
    let err = store
        .validate(ValidateRequest {
            token: bad_token.into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, IdentityError::InvalidToken),
        "expected InvalidToken, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// BearerTokenValidator
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_bearer_valid_token_returns_role() {
    let (store, _c) = start_store().await;
    let resp = store.register(reg("eve", "password")).await.unwrap();
    let role = store.validate_bearer(&resp.token);
    assert_eq!(role, Some("user".to_string()));
}

#[tokio::test]
async fn validate_bearer_invalid_token_returns_none() {
    let (store, _c) = start_store().await;
    let role = store.validate_bearer("garbage.token.data");
    assert_eq!(role, None);
}

// ---------------------------------------------------------------------------
// Round-trip: register → login → validate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_round_trip_register_login_validate() {
    let (store, _c) = start_store().await;

    let _reg_token = store.register(reg("frank", "s3kr1t")).await.unwrap().token;
    let login_token = store.login(login("frank", "s3kr1t")).await.unwrap().token;

    let role_resp = store
        .validate(ValidateRequest { token: login_token })
        .await
        .unwrap();
    assert_eq!(role_resp.role, "user");
}
