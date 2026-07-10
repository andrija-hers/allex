//! A reusable identity engine for projects whose users live in a custom store
//! and whose authorization needs more than a single role string.
//!
//! allex ships four concrete `IdentityV1` stores (in-memory / sqlite / postgres
//! / redb), but each hard-codes its credential model: a `users(handle,
//! password_hash, role)` table with bcrypt and a single role. A project that
//! authenticates against an existing database (a stored proc, an SSO guid, a
//! non-bcrypt scheme) and needs per-user *rights* — not one role — previously
//! had to re-implement the whole `IdentityV1` + [`BearerTokenValidator`] surface,
//! JWT plumbing included (~200 lines), as a fifth store.
//!
//! [`CredentialStore`] is that plumbing, written once and generic over a
//! [`CredentialBackend`]. A project implements only the two genuinely
//! project-specific methods — verify a credential payload, and re-resolve a user
//! by handle — and gets login / refresh / logout / bearer validation for free,
//! built on the shared [`crate::tokens`] JWT engine. Two properties fall out of
//! the design:
//!
//! - **Typed profile claims.** [`AuthenticatedUser::profile`] is an opaque,
//!   project-defined payload (e.g. a JSON string of rights flags) baked into the
//!   access token and re-emitted as the third `\x1e`-segment of
//!   [`BearerTokenValidator::validate_bearer`], so it reaches
//!   `allex_serve::caller_profile()`. allex never interprets it.
//! - **Rights freshness on refresh.** [`CredentialStore::refresh`] calls
//!   [`CredentialBackend::resolve_user`], so a token refresh re-reads the user's
//!   *current* role and profile — rights changes propagate at the refresh
//!   boundary instead of being frozen for the refresh token's lifetime.
//!
//! Session state (opaque refresh tokens, revoked access tokens) is kept
//! **in-memory** in this version — correct for a single-instance service. A
//! durable / multi-instance `SessionStore` seam can be added later without
//! changing the [`CredentialBackend`] contract.

use std::{collections::HashMap, future::Future, sync::Arc};

use tokio::sync::Mutex;
use transport::BearerTokenValidator;

use crate::{IdentityError, TokenResponse, tokens};

/// Field separator between the caller's handle, role, and optional profile in the
/// string [`BearerTokenValidator::validate_bearer`] returns (ASCII record
/// separator, `0x1E`). Matches `allex_serve::CallerContext::parse`.
const CALLER_FIELD_SEP: char = '\u{1e}';

/// A user authenticated by a [`CredentialBackend`].
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// Stable user handle (the token subject).
    pub handle: String,
    /// The caller role checked by `#[allowed_roles(..)]` on contract methods.
    /// Projects that authorize purely via `profile` can pass a constant here.
    pub role: String,
    /// Opaque, project-defined claims (e.g. a JSON string of rights flags) baked
    /// into the access token and surfaced via `allex_serve::caller_profile()`.
    /// `None` for role-only projects.
    pub profile: Option<String>,
}

/// The project-specific half of an identity store: turn a credential payload into
/// an [`AuthenticatedUser`], and re-resolve a user's current role/profile by
/// handle. Everything else (JWT issue/validate, refresh rotation, revocation,
/// bearer validation) is provided by [`CredentialStore`].
pub trait CredentialBackend: Send + Sync + 'static {
    /// The project-defined credential payload (e.g. `{handle, password, station}`
    /// or an SSO guid). [`CredentialStore`] never inspects it — it is handed
    /// straight to [`verify_credentials`](CredentialBackend::verify_credentials).
    type Credentials: Send + 'static;

    /// Authenticates a credential payload. `Ok(None)` means the credentials are
    /// invalid (the store maps this to [`IdentityError::InvalidCredentials`]);
    /// `Ok(Some(user))` on success. Reserve `Err` for backend faults (DB down),
    /// not for a wrong password.
    fn verify_credentials(
        &self,
        credentials: Self::Credentials,
    ) -> impl Future<Output = Result<Option<AuthenticatedUser>, IdentityError>> + Send;

    /// Re-resolves a user's *current* role and profile by handle. Called on every
    /// [`refresh`](CredentialStore::refresh) so rights changes propagate at the
    /// refresh boundary. `Ok(None)` if the user no longer exists (the refresh is
    /// then rejected).
    fn resolve_user(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<Option<AuthenticatedUser>, IdentityError>> + Send;
}

struct CredentialStoreInner<B: CredentialBackend> {
    backend: B,
    secret: String,
    refresh_ttl_secs: u64,
    /// opaque refresh token -> (handle, expiry_unix_secs)
    refresh_tokens: Mutex<HashMap<String, (String, u64)>>,
    /// revoked access token -> expiry_unix_secs (kept only until natural expiry)
    revoked_access: Mutex<HashMap<String, u64>>,
}

/// Generic identity engine over a [`CredentialBackend`]. Cheaply cloneable
/// (shared `Arc` inner) so the same instance can back both a generated
/// `IdentityV1`-style responder and `router_with_auth`'s bearer validator.
pub struct CredentialStore<B: CredentialBackend> {
    inner: Arc<CredentialStoreInner<B>>,
}

impl<B: CredentialBackend> Clone for CredentialStore<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: CredentialBackend> CredentialStore<B> {
    /// Creates a store signing tokens with `secret`, using the default refresh
    /// TTL ([`crate::tokens::REFRESH_TOKEN_TTL_SECS`]). Access tokens use
    /// [`crate::tokens::ACCESS_TOKEN_TTL_SECS`].
    pub fn new(backend: B, secret: impl Into<String>) -> Self {
        Self::new_with_refresh_ttl(backend, secret, tokens::REFRESH_TOKEN_TTL_SECS)
    }

    /// Like [`new`](CredentialStore::new) but with an explicit refresh-token TTL.
    pub fn new_with_refresh_ttl(
        backend: B,
        secret: impl Into<String>,
        refresh_ttl_secs: u64,
    ) -> Self {
        Self {
            inner: Arc::new(CredentialStoreInner {
                backend,
                secret: secret.into(),
                refresh_ttl_secs,
                refresh_tokens: Mutex::new(HashMap::new()),
                revoked_access: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Borrows the underlying backend (e.g. to reach project-specific methods).
    pub fn backend(&self) -> &B {
        &self.inner.backend
    }

    /// Authenticates a credential payload and returns a fresh token pair.
    pub async fn login(
        &self,
        credentials: B::Credentials,
    ) -> Result<TokenResponse, IdentityError> {
        let user = self
            .inner
            .backend
            .verify_credentials(credentials)
            .await?
            .ok_or(IdentityError::InvalidCredentials)?;
        self.issue_for(&user).await
    }

    /// Exchanges a valid refresh token for a new token pair, **re-resolving** the
    /// user's current role/profile so rights changes take effect (rotates the
    /// refresh token).
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse, IdentityError> {
        let handle = {
            let mut map = self.inner.refresh_tokens.lock().await;
            match map.remove(refresh_token) {
                Some((handle, expiry)) if expiry > now_secs() => handle,
                _ => return Err(IdentityError::InvalidRefreshToken),
            }
        };
        let user = self
            .inner
            .backend
            .resolve_user(&handle)
            .await?
            .ok_or(IdentityError::InvalidRefreshToken)?;
        self.issue_for(&user).await
    }

    /// Revokes an access token (until its natural expiry) and/or drops a refresh
    /// token, so both are rejected on subsequent requests.
    pub async fn logout(&self, access_token: Option<&str>, refresh_token: Option<&str>) {
        if let Some(at) = access_token {
            let exp = tokens::validate_access_token(&self.inner.secret, at)
                .map(|v| v.expires_at_unix)
                .unwrap_or_else(|_| now_secs());
            let mut revoked = self.inner.revoked_access.lock().await;
            revoked.retain(|_, e| *e > now_secs());
            revoked.insert(at.to_owned(), exp);
        }
        if let Some(rt) = refresh_token {
            self.inner.refresh_tokens.lock().await.remove(rt);
        }
    }

    async fn issue_for(&self, user: &AuthenticatedUser) -> Result<TokenResponse, IdentityError> {
        let token = tokens::issue_access_token_with_profile(
            &self.inner.secret,
            &user.handle,
            &user.role,
            user.profile.as_deref(),
        )?;
        let refresh = uuid::Uuid::new_v4().to_string();
        let expiry = now_secs() + self.inner.refresh_ttl_secs;
        {
            let mut map = self.inner.refresh_tokens.lock().await;
            map.retain(|_, (_, e)| *e > now_secs());
            map.insert(refresh.clone(), (user.handle.clone(), expiry));
        }
        Ok(TokenResponse {
            token,
            refresh_token: Some(refresh),
            handle: user.handle.clone(),
            role: user.role.clone(),
        })
    }
}

impl<B: CredentialBackend> BearerTokenValidator for CredentialStore<B> {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        // Non-blocking revocation check on the request path (mirrors the stock
        // stores). A missed lock momentarily treats the token as live; the JWT
        // signature + expiry are still enforced below.
        let revoked = self
            .inner
            .revoked_access
            .try_lock()
            .map(|set| set.get(token).is_some_and(|exp| *exp > now_secs()))
            .unwrap_or(false);
        if revoked {
            return None;
        }
        let v = tokens::validate_access_token(&self.inner.secret, token).ok()?;
        Some(match v.profile {
            Some(profile) => {
                format!("{}{CALLER_FIELD_SEP}{}{CALLER_FIELD_SEP}{}", v.handle, v.role, profile)
            }
            None => format!("{}{CALLER_FIELD_SEP}{}", v.handle, v.role),
        })
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    /// A minimal backend whose user table can be mutated between calls, so a test
    /// can prove that refresh re-resolves the *current* role/profile.
    #[derive(Clone, Default)]
    struct TestBackend {
        // handle -> (password, role, profile)
        users: Arc<Mutex<Map<String, (String, String, Option<String>)>>>,
    }

    impl TestBackend {
        async fn set(&self, handle: &str, password: &str, role: &str, profile: Option<&str>) {
            self.users.lock().await.insert(
                handle.to_owned(),
                (password.to_owned(), role.to_owned(), profile.map(str::to_owned)),
            );
        }
    }

    struct Creds {
        handle: String,
        password: String,
    }

    impl CredentialBackend for TestBackend {
        type Credentials = Creds;

        async fn verify_credentials(
            &self,
            credentials: Creds,
        ) -> Result<Option<AuthenticatedUser>, IdentityError> {
            let users = self.users.lock().await;
            Ok(users.get(&credentials.handle).and_then(|(pw, role, profile)| {
                (*pw == credentials.password).then(|| AuthenticatedUser {
                    handle: credentials.handle.clone(),
                    role: role.clone(),
                    profile: profile.clone(),
                })
            }))
        }

        async fn resolve_user(
            &self,
            handle: &str,
        ) -> Result<Option<AuthenticatedUser>, IdentityError> {
            let users = self.users.lock().await;
            Ok(users.get(handle).map(|(_, role, profile)| AuthenticatedUser {
                handle: handle.to_owned(),
                role: role.clone(),
                profile: profile.clone(),
            }))
        }
    }

    fn store(backend: TestBackend) -> CredentialStore<TestBackend> {
        CredentialStore::new(backend, "test-secret")
    }

    #[tokio::test]
    async fn login_bad_credentials_is_invalid() {
        let backend = TestBackend::default();
        backend.set("alice", "pw", "user", None).await;
        let s = store(backend);
        let err = s
            .login(Creds { handle: "alice".into(), password: "wrong".into() })
            .await
            .unwrap_err();
        assert!(matches!(err, IdentityError::InvalidCredentials));
    }

    #[tokio::test]
    async fn login_then_validate_bearer_emits_profile_segment() {
        let backend = TestBackend::default();
        backend
            .set("alice", "pw", "user", Some(r#"{"r_admin":true}"#))
            .await;
        let s = store(backend);
        let resp = s
            .login(Creds { handle: "alice".into(), password: "pw".into() })
            .await
            .unwrap();
        let caller = s.validate_bearer(&resp.token).unwrap();
        let parts: Vec<&str> = caller.split(CALLER_FIELD_SEP).collect();
        assert_eq!(parts[0], "alice");
        assert_eq!(parts[1], "user");
        assert_eq!(parts[2], r#"{"r_admin":true}"#);
    }

    #[tokio::test]
    async fn refresh_reresolves_current_rights() {
        let backend = TestBackend::default();
        backend
            .set("alice", "pw", "user", Some(r#"{"r_admin":false}"#))
            .await;
        let s = store(backend.clone());
        let resp = s
            .login(Creds { handle: "alice".into(), password: "pw".into() })
            .await
            .unwrap();

        // An admin grants alice r_admin after she logged in.
        backend
            .set("alice", "pw", "user", Some(r#"{"r_admin":true}"#))
            .await;

        // Refresh re-resolves — the new access token carries the fresh profile.
        let refreshed = s.refresh(resp.refresh_token.as_deref().unwrap()).await.unwrap();
        let caller = s.validate_bearer(&refreshed.token).unwrap();
        assert!(caller.ends_with(r#"{"r_admin":true}"#), "got: {caller}");
    }

    #[tokio::test]
    async fn refresh_token_rotates_and_old_one_is_rejected() {
        let backend = TestBackend::default();
        backend.set("alice", "pw", "user", None).await;
        let s = store(backend);
        let resp = s
            .login(Creds { handle: "alice".into(), password: "pw".into() })
            .await
            .unwrap();
        let first_rt = resp.refresh_token.unwrap();
        let _ = s.refresh(&first_rt).await.unwrap();
        // The consumed refresh token no longer works.
        assert!(matches!(
            s.refresh(&first_rt).await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
    }

    #[tokio::test]
    async fn logout_revokes_access_token() {
        let backend = TestBackend::default();
        backend.set("alice", "pw", "user", None).await;
        let s = store(backend);
        let resp = s
            .login(Creds { handle: "alice".into(), password: "pw".into() })
            .await
            .unwrap();
        assert!(s.validate_bearer(&resp.token).is_some());
        s.logout(Some(&resp.token), resp.refresh_token.as_deref()).await;
        assert!(s.validate_bearer(&resp.token).is_none());
    }
}
