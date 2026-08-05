//! Integration tests for `allex-identity-redb`.
//!
//! Run with:
//!
//! ```text
//! cargo test -p allex-identity-redb
//! ```

use allex_identity_redb::RedbIdentityStore;
use identity::{
    AccountStore, IdentityError, IdentityV1, LoginRequest, LogoutRequest, RefreshRequest,
    RegisterRequest, ValidateRequest,
};
use message_templates::{
    ForgotPasswordMessageRequest, TemplateChannel, UpsertMessageTemplateRequest,
};
use tempfile::tempdir;
use transport::BearerTokenValidator;

const SECRET: &str = "test-jwt-secret-key-for-redb-integration-tests";

async fn start_store() -> (RedbIdentityStore, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir should be created");
    let store = RedbIdentityStore::new(dir.path().join("identity.redb"), SECRET)
        .expect("redb store should open");
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

fn login_req(handle: &str, password: &str) -> LoginRequest {
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
    let resp = store
        .login(login_req("carol", "my-password"))
        .await
        .unwrap();
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
    let err = store.login(login_req("dave", "wrong")).await.unwrap_err();
    assert!(matches!(err, IdentityError::InvalidCredentials));
}

#[tokio::test]
async fn login_unknown_handle_returns_invalid_credentials() {
    let (store, _dir) = start_store().await;
    let err = store.login(login_req("nobody", "pass")).await.unwrap_err();
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
            refresh_token: "bogus".into(),
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
    assert_eq!(
        store.validate_bearer(&resp.token),
        Some("eve\x1euser".to_string())
    );
}

#[tokio::test]
async fn validate_bearer_invalid_token_returns_none() {
    let (store, _dir) = start_store().await;
    assert_eq!(store.validate_bearer("garbage.token.data"), None);
}

#[tokio::test]
async fn logout_revokes_access_and_refresh_token() {
    let (store, _dir) = start_store().await;
    let resp = store.register(reg("frank_l", "s3kr1t")).await.unwrap();
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
async fn full_round_trip_register_login_validate() {
    let (store, _dir) = start_store().await;
    let _ = store.register(reg("grace", "s3kr1t")).await.unwrap();
    let token = store
        .login(login_req("grace", "s3kr1t"))
        .await
        .unwrap()
        .token;
    let role_resp = store.validate(ValidateRequest { token }).await.unwrap();
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

// ---------------------------------------------------------------------------
// AccountStore: list / set_role / delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_accounts_returns_every_account_ordered_by_handle() {
    let (store, _dir) = start_store().await;
    for (handle, role) in [("zoe", "user"), ("adam", "admin"), ("mia", "operator")] {
        store
            .register(RegisterRequest {
                handle: handle.into(),
                password: "pw".into(),
                role: role.into(),
            })
            .await
            .unwrap();
    }
    let accounts = store.list_accounts().await.unwrap();
    assert_eq!(
        accounts
            .iter()
            .map(|a| (a.handle.as_str(), a.role.as_str()))
            .collect::<Vec<_>>(),
        vec![("adam", "admin"), ("mia", "operator"), ("zoe", "user")]
    );
}

#[tokio::test]
async fn set_role_changes_the_role_and_reports_unknown_user() {
    let (store, _dir) = start_store().await;
    store.register(reg("frank", "pw")).await.unwrap();
    store.set_role("frank", "operator").await.unwrap();
    assert_eq!(
        store.get_account("frank").await.unwrap().unwrap().role,
        "operator"
    );
    assert_eq!(
        store.login(login_req("frank", "pw")).await.unwrap().role,
        "operator"
    );
    assert!(matches!(
        store.set_role("ghost", "operator").await,
        Err(IdentityError::UserNotFound)
    ));
}

// A refresh token remembers the role it was minted with, so unless set_role
// drops it, a demoted user refreshes straight back into the role they lost.
#[tokio::test]
async fn set_role_revokes_refresh_tokens_so_a_demotion_cannot_be_refreshed_away() {
    let (store, _dir) = start_store().await;
    store
        .register(RegisterRequest {
            handle: "grace".into(),
            password: "pw".into(),
            role: "admin".into(),
        })
        .await
        .unwrap();
    let refresh_token = store
        .issue_session("grace")
        .await
        .unwrap()
        .refresh_token
        .unwrap();
    store.set_role("grace", "user").await.unwrap();
    assert!(matches!(
        store.refresh(RefreshRequest { refresh_token }).await,
        Err(IdentityError::InvalidRefreshToken)
    ));
}

#[tokio::test]
async fn delete_account_removes_the_user_and_reports_unknown_user() {
    let (store, _dir) = start_store().await;
    store.register(reg("henry", "pw")).await.unwrap();
    store.delete_account("henry").await.unwrap();
    assert!(store.get_account("henry").await.unwrap().is_none());
    assert!(matches!(
        store.login(login_req("henry", "pw")).await,
        Err(IdentityError::InvalidCredentials)
    ));
    assert!(matches!(
        store.delete_account("henry").await,
        Err(IdentityError::UserNotFound)
    ));
}

// redb has no foreign keys, so the tokens can only go explicitly — and must:
// `refresh` reads the token's own row without re-checking the user exists, so a
// stranded token is a deleted account that still mints sessions.
#[tokio::test]
async fn delete_account_revokes_refresh_tokens_so_a_deleted_user_cannot_mint_sessions() {
    let (store, _dir) = start_store().await;
    store.register(reg("iris", "pw")).await.unwrap();
    let refresh_token = store
        .issue_session("iris")
        .await
        .unwrap()
        .refresh_token
        .unwrap();
    store.delete_account("iris").await.unwrap();
    assert!(matches!(
        store.refresh(RefreshRequest { refresh_token }).await,
        Err(IdentityError::InvalidRefreshToken)
    ));
}
