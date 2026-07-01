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
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, RefreshRequest, RegisterRequest,
    ValidateRequest,
};
use message_templates::{
    ForgotPasswordMessageRequest, TemplateChannel, UpsertMessageTemplateRequest,
};
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
    assert!(resp.refresh_token.is_some());

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
    assert!(resp.refresh_token.is_some());

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
// Refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn refresh_returns_new_token_pair() {
    let (store, _c) = start_store().await;
    let login_resp = store.register(reg("eve_r", "pass")).await.unwrap();
    let refresh_token = login_resp.refresh_token.unwrap();

    let refreshed = store
        .refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        })
        .await
        .unwrap();
    assert!(!refreshed.token.is_empty());
    assert!(refreshed.refresh_token.is_some());
    // New refresh token should differ from the old one (rotation)
    assert_ne!(
        refreshed.refresh_token.as_deref(),
        Some(refresh_token.as_str())
    );
}

#[tokio::test]
async fn refresh_token_reuse_within_grace_returns_successor() {
    // Single-use rotation WITH a grace window: the first use rotates the token
    // and mints a successor; re-presenting the consumed token within the grace
    // window returns that SAME successor (so a racing tab / retry stays signed
    // in) rather than minting a new, divergent chain or erroring out.
    let (store, _c) = start_store().await;
    let login_resp = store.register(reg("eve_r2", "pass")).await.unwrap();
    let refresh_token = login_resp.refresh_token.unwrap();

    let first = store
        .refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        })
        .await
        .unwrap();
    let successor = first
        .refresh_token
        .expect("first refresh mints a successor");

    let second = store
        .refresh(RefreshRequest { refresh_token })
        .await
        .expect("reuse within grace returns the successor, not an error");
    assert_eq!(
        second.refresh_token.as_deref(),
        Some(successor.as_str()),
        "within-grace reuse must hand back the same successor token"
    );
}

#[tokio::test]
async fn concurrent_refresh_of_same_token_converges_on_one_successor() {
    // Two tabs firing the same refresh at once: exactly one wins the atomic
    // claim and mints the successor; the other falls to the grace path and must
    // get that SAME successor back (never InvalidRefreshToken, never a second,
    // divergent chain). This is the race the single-statement rotation closes.
    let (store, _c) = start_store().await;
    let login_resp = store.register(reg("eve_r3", "pass")).await.unwrap();
    let refresh_token = login_resp.refresh_token.unwrap();

    let (a, b) = tokio::join!(
        store.refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        }),
        store.refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        }),
    );
    let a = a.expect("one concurrent refresh wins the claim");
    let b = b.expect("the other concurrent refresh follows grace, not an error");
    assert_eq!(
        a.refresh_token, b.refresh_token,
        "both concurrent refreshes must converge on the same successor token"
    );
    assert_ne!(
        a.refresh_token.as_deref(),
        Some(refresh_token.as_str()),
        "the successor must differ from the consumed token"
    );
}

#[tokio::test]
async fn refresh_invalid_token_returns_error() {
    let (store, _c) = start_store().await;
    let err = store
        .refresh(RefreshRequest {
            refresh_token: "bogus-token".into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, IdentityError::InvalidRefreshToken),
        "expected InvalidRefreshToken, got {err:?}"
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
    let bad_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.\
        eyJzdWIiOiJ1c2VyIiwicm9sZSI6InVzZXIiLCJleHAiOjk5OTk5OTk5OTl9.\
        WRONG_SIGNATURE_HERE";
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
async fn validate_bearer_valid_token_returns_identity() {
    let (store, _c) = start_store().await;
    let resp = store.register(reg("eve", "password")).await.unwrap();
    let role = store.validate_bearer(&resp.token);
    assert_eq!(role, Some("eve\x1euser".to_string()));
}

#[tokio::test]
async fn validate_bearer_invalid_token_returns_none() {
    let (store, _c) = start_store().await;
    let role = store.validate_bearer("garbage.token.data");
    assert_eq!(role, None);
}

// ---------------------------------------------------------------------------
// Logout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logout_revokes_access_and_refresh_token() {
    let (store, _c) = start_store().await;
    let resp = store.register(reg("frank", "s3kr1t")).await.unwrap();
    let access = resp.token.clone();
    let refresh = resp.refresh_token.clone().unwrap();

    store
        .logout(LogoutRequest {
            token: Some(access.clone()),
            refresh_token: Some(refresh.clone()),
        })
        .await
        .unwrap();

    // Access token should now be rejected by bearer validator
    assert_eq!(store.validate_bearer(&access), None);

    // Refresh token should now be rejected
    let err = store
        .refresh(RefreshRequest {
            refresh_token: refresh,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, IdentityError::InvalidRefreshToken));
}

// ---------------------------------------------------------------------------
// Round-trip: register -> login -> validate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_round_trip_register_login_validate() {
    let (store, _c) = start_store().await;

    let _reg_token = store.register(reg("grace", "s3kr1t")).await.unwrap().token;
    let login_token = store.login(login("grace", "s3kr1t")).await.unwrap().token;

    let role_resp = store
        .validate(ValidateRequest { token: login_token })
        .await
        .unwrap();
    assert_eq!(role_resp.role, "user");
}

#[tokio::test]
async fn forgot_password_template_is_loaded_from_storage() {
    let (store, _c) = start_store().await;
    store
        .upsert_message_template(UpsertMessageTemplateRequest {
            template_key: "forgot_password".into(),
            channel: TemplateChannel::Email,
            body: "Hi {{ handle }}, reset password via {{ reset_url }}".into(),
        })
        .await
        .unwrap();

    let msg = store
        .build_forgot_password_message(ForgotPasswordMessageRequest {
            handle: "alice".into(),
            channel: TemplateChannel::Email,
            reset_url: "https://example/reset?t=abc".into(),
        })
        .await
        .unwrap();

    assert_eq!(
        msg.message,
        "Hi alice, reset password via https://example/reset?t=abc"
    );
}
