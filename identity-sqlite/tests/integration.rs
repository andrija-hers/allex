//! Integration tests for `allex-identity-sqlite`.
//!
//! Run with:
//!
//! ```text
//! cargo test -p allex-identity-sqlite
//! ```

use allex_identity_sqlite::SqliteIdentityStore;
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, RefreshRequest, RegisterRequest,
    ValidateRequest,
};
use message_templates::{
    ForgotPasswordMessageRequest, TemplateChannel, UpsertMessageTemplateRequest,
};
use tempfile::tempdir;
use transport::BearerTokenValidator;

const SECRET: &str = "test-jwt-secret-key-for-integration-tests";

async fn start_store() -> (SqliteIdentityStore, tempfile::TempDir) {
    let dir = tempdir().expect("temp dir should be created");
    let store = SqliteIdentityStore::new(dir.path().join("identity.db"), SECRET)
        .expect("valid sqlite path");
    store.setup().await.expect("schema setup must succeed");
    (store, dir)
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

#[tokio::test]
async fn setup_is_idempotent() {
    let (store, _dir) = start_store().await;
    store
        .setup()
        .await
        .expect("second setup call must be idempotent");
}

#[tokio::test]
async fn register_creates_user_and_returns_valid_jwt() {
    let (store, _dir) = start_store().await;
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
    let (store, _dir) = start_store().await;
    store.register(reg("bob", "pass1")).await.unwrap();
    let err = store.register(reg("bob", "pass2")).await.unwrap_err();
    assert!(matches!(err, IdentityError::HandleAlreadyTaken));
}

#[tokio::test]
async fn login_valid_credentials_returns_jwt() {
    let (store, _dir) = start_store().await;
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
    let (store, _dir) = start_store().await;
    store.register(reg("dave", "correct")).await.unwrap();
    let err = store.login(login("dave", "wrong")).await.unwrap_err();
    assert!(matches!(err, IdentityError::InvalidCredentials));
}

#[tokio::test]
async fn login_unknown_handle_returns_invalid_credentials() {
    let (store, _dir) = start_store().await;
    let err = store.login(login("nobody", "pass")).await.unwrap_err();
    assert!(matches!(err, IdentityError::InvalidCredentials));
}

#[tokio::test]
async fn refresh_returns_new_token_pair() {
    let (store, _dir) = start_store().await;
    let resp = store.register(reg("eve_r", "pass")).await.unwrap();
    let refresh_token = resp.refresh_token.unwrap();

    let refreshed = store
        .refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        })
        .await
        .unwrap();
    assert!(!refreshed.token.is_empty());
    assert!(refreshed.refresh_token.is_some());
    assert_ne!(
        refreshed.refresh_token.as_deref(),
        Some(refresh_token.as_str())
    );
}

#[tokio::test]
async fn refresh_token_cannot_be_reused() {
    let (store, _dir) = start_store().await;
    let resp = store.register(reg("eve_r2", "pass")).await.unwrap();
    let refresh_token = resp.refresh_token.unwrap();

    store
        .refresh(RefreshRequest {
            refresh_token: refresh_token.clone(),
        })
        .await
        .unwrap();

    let err = store
        .refresh(RefreshRequest { refresh_token })
        .await
        .unwrap_err();
    assert!(matches!(err, IdentityError::InvalidRefreshToken));
}

#[tokio::test]
async fn refresh_invalid_token_returns_error() {
    let (store, _dir) = start_store().await;
    let err = store
        .refresh(RefreshRequest {
            refresh_token: "bogus-token".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, IdentityError::InvalidRefreshToken));
}

#[tokio::test]
async fn validate_invalid_token_returns_error() {
    let (store, _dir) = start_store().await;
    let err = store
        .validate(ValidateRequest {
            token: "not.a.jwt".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, IdentityError::InvalidToken));
}

#[tokio::test]
async fn validate_bearer_valid_token_returns_identity() {
    let (store, _dir) = start_store().await;
    let resp = store.register(reg("eve", "password")).await.unwrap();
    let role = store.validate_bearer(&resp.token);
    assert_eq!(role, Some("eve\x1euser".to_string()));
}

#[tokio::test]
async fn validate_bearer_invalid_token_returns_none() {
    let (store, _dir) = start_store().await;
    let role = store.validate_bearer("garbage.token.data");
    assert_eq!(role, None);
}

#[tokio::test]
async fn logout_revokes_access_and_refresh_token() {
    let (store, _dir) = start_store().await;
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

    assert_eq!(store.validate_bearer(&access), None);

    let err = store
        .refresh(RefreshRequest {
            refresh_token: refresh,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, IdentityError::InvalidRefreshToken));
}

#[tokio::test]
async fn revoked_access_token_persists_across_restart() {
    let (store, dir) = start_store().await;
    let resp = store.register(reg("henry", "pw")).await.unwrap();
    let access = resp.token.clone();

    store
        .logout(LogoutRequest {
            token: Some(access.clone()),
            refresh_token: None,
        })
        .await
        .unwrap();

    let reopened = SqliteIdentityStore::new(dir.path().join("identity.db"), SECRET).unwrap();
    reopened.setup().await.unwrap();

    assert_eq!(reopened.validate_bearer(&access), None);
}

#[tokio::test]
async fn full_round_trip_register_login_validate() {
    let (store, _dir) = start_store().await;

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
    let (store, _dir) = start_store().await;
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
