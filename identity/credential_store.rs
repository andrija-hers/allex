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
//! Session state (opaque refresh tokens, revoked access tokens) lives behind the
//! [`SessionStore`] seam. The default, [`MemorySessionStore`], keeps it in memory
//! — correct for a single-instance service whose sessions may end with the
//! process. A host whose users must stay signed in across a restart or an
//! upgrade plugs in a durable store via [`CredentialStore::new_with_options`].
//! A store is only ever handed the SHA-256 of a token (see [`session_key`]), so
//! whatever it persists cannot be replayed as a credential.
//!
//! An optional **absolute session cap** ([`SessionOptions::max_session_secs`])
//! bounds how long one sign-in can be kept alive by refreshing: every refresh
//! entry carries the time of the original login, unchanged through rotation.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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

/// One live refresh token, as a [`SessionStore`] keeps it (under the token's
/// [`session_key`], never the token itself).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshSession {
    /// The user the token was issued to (re-resolved on refresh).
    pub handle: String,
    /// Unix seconds from which the token is refused (sliding: a refresh issues a
    /// new token with a fresh expiry).
    pub expires_at: u64,
    /// Unix seconds of the original login (credentials or SSO) — carried
    /// unchanged through every rotation, so an absolute cap can be enforced.
    pub login_at: u64,
}

/// Where [`CredentialStore`] keeps session state: live refresh tokens and
/// revoked access tokens, each under an opaque key (the hex SHA-256 of the
/// token, see [`session_key`]).
///
/// Methods are synchronous and infallible: a store backs every login, refresh,
/// logout and authenticated request, and a durable store that cannot persist
/// should degrade (log, keep serving from memory) rather than fail sign-in.
/// Each method must be atomic with respect to the others — in particular
/// [`take_refresh`](SessionStore::take_refresh) must hand a given key out at
/// most once, which is what keeps a refresh token single-use when two refreshes
/// of the same token race.
pub trait SessionStore: Send + Sync + 'static {
    /// Drops every refresh session expired at `now` (`expires_at <= now`), then
    /// stores `session` under `key`.
    fn put_refresh(&self, key: &str, session: RefreshSession, now: u64);

    /// Removes and returns the refresh session under `key`, expired or not (the
    /// caller checks).
    fn take_refresh(&self, key: &str) -> Option<RefreshSession>;

    /// Removes and returns the refresh session under `key` only when it belongs
    /// to `handle` (expired or not: the caller checks). A session of another
    /// user is left in place and `None` returned, so a refresh token presented
    /// by the wrong caller is refused without being consumed.
    ///
    /// The default takes and, on a mismatch, puts the session back: correct,
    /// but with a window in which a concurrent refresh by the rightful owner
    /// misses it. A store that can test-and-remove under one lock should
    /// override it (the in-memory store does).
    fn take_refresh_for(&self, key: &str, handle: &str) -> Option<RefreshSession> {
        let session = self.take_refresh(key)?;
        if session.handle == handle {
            return Some(session);
        }
        // `now = 0`: the put-back sweeps nothing.
        self.put_refresh(key, session, 0);
        None
    }

    /// Removes the refresh session under `key`, if any.
    fn remove_refresh(&self, key: &str);

    /// Drops every revocation expired at `now`, then records `key` as revoked
    /// until `expires_at` (the access token's own expiry).
    fn revoke_access(&self, key: &str, expires_at: u64, now: u64);

    /// Whether `key` is revoked and the revocation has not expired at `now`.
    fn is_revoked(&self, key: &str, now: u64) -> bool;
}

/// The default [`SessionStore`]: two in-memory maps, lost when the process ends.
#[derive(Debug, Default)]
pub struct MemorySessionStore {
    refresh: Mutex<HashMap<String, RefreshSession>>,
    revoked: Mutex<HashMap<String, u64>>,
}

impl MemorySessionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl SessionStore for MemorySessionStore {
    fn put_refresh(&self, key: &str, session: RefreshSession, now: u64) {
        let mut map = lock(&self.refresh);
        map.retain(|_, s| s.expires_at > now);
        map.insert(key.to_owned(), session);
    }

    fn take_refresh(&self, key: &str) -> Option<RefreshSession> {
        lock(&self.refresh).remove(key)
    }

    fn take_refresh_for(&self, key: &str, handle: &str) -> Option<RefreshSession> {
        let mut map = lock(&self.refresh);
        if map.get(key)?.handle != handle {
            return None;
        }
        map.remove(key)
    }

    fn remove_refresh(&self, key: &str) {
        lock(&self.refresh).remove(key);
    }

    fn revoke_access(&self, key: &str, expires_at: u64, now: u64) {
        let mut map = lock(&self.revoked);
        map.retain(|_, e| *e > now);
        map.insert(key.to_owned(), expires_at);
    }

    fn is_revoked(&self, key: &str, now: u64) -> bool {
        lock(&self.revoked).get(key).is_some_and(|e| *e > now)
    }
}

/// A poisoned lock still guards a consistent map (every critical section is a
/// single map operation), so recover the guard rather than fail sign-in.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The key a [`SessionStore`] sees for a token: the lowercase hex SHA-256 of the
/// token string. One-way, so a store's contents (a file, a table) cannot be
/// replayed as a refresh or access token.
pub fn session_key(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

/// Session settings for [`CredentialStore::new_with_options`].
#[derive(Clone)]
pub struct SessionOptions {
    /// Refresh-token lifetime from issue (sliding: each refresh re-issues).
    pub refresh_ttl_secs: u64,
    /// Absolute cap on one sign-in: a refresh is refused once more than this many
    /// seconds have passed since the original login, however recently the token
    /// was rotated. `None` = no cap.
    pub max_session_secs: Option<u64>,
    /// Where refresh tokens and revocations are kept.
    pub store: Arc<dyn SessionStore>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            refresh_ttl_secs: tokens::REFRESH_TOKEN_TTL_SECS,
            max_session_secs: None,
            store: Arc::new(MemorySessionStore::new()),
        }
    }
}

struct CredentialStoreInner<B: CredentialBackend> {
    backend: B,
    secret: String,
    refresh_ttl_secs: u64,
    max_session_secs: Option<u64>,
    /// Refresh sessions + revoked access tokens, keyed by [`session_key`].
    sessions: Arc<dyn SessionStore>,
}

/// The two token pairs [`CredentialStore::fork`] issues: one for the window
/// that asked (its old refresh token is spent), one for the window it hands a
/// session to. The two refresh chains are independent from here on.
#[derive(Debug, Clone)]
pub struct ForkedTokens {
    /// The forking caller's new pair.
    pub opener: TokenResponse,
    /// The new window's pair.
    pub child: TokenResponse,
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
        Self::new_with_options(
            backend,
            secret,
            SessionOptions {
                refresh_ttl_secs,
                ..SessionOptions::default()
            },
        )
    }

    /// Like [`new`](CredentialStore::new) with explicit [`SessionOptions`]: a
    /// custom (e.g. durable) [`SessionStore`] and/or an absolute session cap.
    pub fn new_with_options(
        backend: B,
        secret: impl Into<String>,
        options: SessionOptions,
    ) -> Self {
        Self {
            inner: Arc::new(CredentialStoreInner {
                backend,
                secret: secret.into(),
                refresh_ttl_secs: options.refresh_ttl_secs,
                max_session_secs: options.max_session_secs,
                sessions: options.store,
            }),
        }
    }

    /// Borrows the underlying backend (e.g. to reach project-specific methods).
    pub fn backend(&self) -> &B {
        &self.inner.backend
    }

    /// Authenticates a credential payload and returns a fresh token pair.
    pub async fn login(&self, credentials: B::Credentials) -> Result<TokenResponse, IdentityError> {
        let user = self
            .inner
            .backend
            .verify_credentials(credentials)
            .await?
            .ok_or(IdentityError::InvalidCredentials)?;
        self.issue_for(&user, now_secs()).await
    }

    /// Exchanges a valid refresh token for a new token pair, **re-resolving** the
    /// user's current role/profile so rights changes take effect (rotates the
    /// refresh token). The token is consumed first and checked second, so it is
    /// single-use even when two refreshes race or it turns out expired. Past the
    /// absolute cap ([`SessionOptions::max_session_secs`]) the refresh is refused
    /// exactly like an expired token.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse, IdentityError> {
        let session = self
            .inner
            .sessions
            .take_refresh(&session_key(refresh_token))
            .ok_or(IdentityError::InvalidRefreshToken)?;
        let now = now_secs();
        let capped = self
            .inner
            .max_session_secs
            .is_some_and(|cap| now.saturating_sub(session.login_at) > cap);
        if session.expires_at <= now || capped {
            return Err(IdentityError::InvalidRefreshToken);
        }
        let user = self
            .inner
            .backend
            .resolve_user(&session.handle)
            .await?
            .ok_or(IdentityError::InvalidRefreshToken)?;
        self.issue_for(&user, session.login_at).await
    }

    /// Splits one sign-in into two independent sessions: consumes `refresh_token`
    /// and issues **two** token pairs, each with its own refresh chain, both
    /// carrying the original login time (so the absolute cap,
    /// [`SessionOptions::max_session_secs`], still counts from the one sign-in).
    /// Used to hand a second window a session of its own instead of sharing,
    /// and racing over, one single-use refresh chain.
    ///
    /// `expected_handle` is the user the caller has already authenticated as
    /// (e.g. the bearer's handle): a token issued to anyone else is refused
    /// **without** being consumed, so a stolen refresh token can be neither
    /// forked nor burnt by another user. Otherwise the checks are exactly
    /// [`refresh`](CredentialStore::refresh)'s (consumed first, then expiry and
    /// cap, then the user re-resolved) and every refusal is the same plain
    /// [`IdentityError::InvalidRefreshToken`].
    pub async fn fork(
        &self,
        refresh_token: &str,
        expected_handle: &str,
    ) -> Result<ForkedTokens, IdentityError> {
        let session = self
            .inner
            .sessions
            .take_refresh_for(&session_key(refresh_token), expected_handle)
            .ok_or(IdentityError::InvalidRefreshToken)?;
        let now = now_secs();
        let capped = self
            .inner
            .max_session_secs
            .is_some_and(|cap| now.saturating_sub(session.login_at) > cap);
        if session.expires_at <= now || capped {
            return Err(IdentityError::InvalidRefreshToken);
        }
        let user = self
            .inner
            .backend
            .resolve_user(&session.handle)
            .await?
            .ok_or(IdentityError::InvalidRefreshToken)?;
        Ok(ForkedTokens {
            opener: self.issue_for(&user, session.login_at).await?,
            child: self.issue_for(&user, session.login_at).await?,
        })
    }

    /// Revokes an access token (until its natural expiry) and/or drops a refresh
    /// token, so both are rejected on subsequent requests.
    pub async fn logout(&self, access_token: Option<&str>, refresh_token: Option<&str>) {
        if let Some(at) = access_token {
            let exp = tokens::validate_access_token(&self.inner.secret, at)
                .map(|v| v.expires_at_unix)
                .unwrap_or_else(|_| now_secs());
            self.inner
                .sessions
                .revoke_access(&session_key(at), exp, now_secs());
        }
        if let Some(rt) = refresh_token {
            self.inner.sessions.remove_refresh(&session_key(rt));
        }
    }

    /// Drops a refresh token on its own, with no access token involved.
    ///
    /// The counterpart of [`IdentityV1::revoke_refresh_token`](crate::IdentityV1::revoke_refresh_token)
    /// for hosts built on this store: the case it serves is a session that can no
    /// longer authenticate, where requiring a live access token would mean the
    /// longer-lived credential is the one you cannot revoke. Says nothing about
    /// whether the token existed — a host exposing this publicly must not leak
    /// which tokens are live.
    pub async fn revoke_refresh_token(&self, refresh_token: &str) {
        self.inner
            .sessions
            .remove_refresh(&session_key(refresh_token));
    }

    /// Issues a token pair for `user`; `login_at` is the original login's time
    /// (now for a login, the consumed entry's for a refresh).
    async fn issue_for(
        &self,
        user: &AuthenticatedUser,
        login_at: u64,
    ) -> Result<TokenResponse, IdentityError> {
        let token = tokens::issue_access_token_with_profile(
            &self.inner.secret,
            &user.handle,
            &user.role,
            user.profile.as_deref(),
        )?;
        let refresh = uuid::Uuid::new_v4().to_string();
        let now = now_secs();
        self.inner.sessions.put_refresh(
            &session_key(&refresh),
            RefreshSession {
                handle: user.handle.clone(),
                expires_at: now + self.inner.refresh_ttl_secs,
                login_at,
            },
            now,
        );
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
        // The revocation check first (a short std-mutex section, never across an
        // await); the JWT signature + expiry are enforced below.
        if self
            .inner
            .sessions
            .is_revoked(&session_key(token), now_secs())
        {
            return None;
        }
        let v = tokens::validate_access_token(&self.inner.secret, token).ok()?;
        Some(match v.profile {
            Some(profile) => {
                format!(
                    "{}{CALLER_FIELD_SEP}{}{CALLER_FIELD_SEP}{}",
                    v.handle, v.role, profile
                )
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
    use tokio::sync::Mutex;

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
                (
                    password.to_owned(),
                    role.to_owned(),
                    profile.map(str::to_owned),
                ),
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
            Ok(users
                .get(&credentials.handle)
                .and_then(|(pw, role, profile)| {
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
            Ok(users
                .get(handle)
                .map(|(_, role, profile)| AuthenticatedUser {
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
            .login(Creds {
                handle: "alice".into(),
                password: "wrong".into(),
            })
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
            .login(Creds {
                handle: "alice".into(),
                password: "pw".into(),
            })
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
            .login(Creds {
                handle: "alice".into(),
                password: "pw".into(),
            })
            .await
            .unwrap();

        // An admin grants alice r_admin after she logged in.
        backend
            .set("alice", "pw", "user", Some(r#"{"r_admin":true}"#))
            .await;

        // Refresh re-resolves — the new access token carries the fresh profile.
        let refreshed = s
            .refresh(resp.refresh_token.as_deref().unwrap())
            .await
            .unwrap();
        let caller = s.validate_bearer(&refreshed.token).unwrap();
        assert!(caller.ends_with(r#"{"r_admin":true}"#), "got: {caller}");
    }

    #[tokio::test]
    async fn refresh_token_rotates_and_old_one_is_rejected() {
        let backend = TestBackend::default();
        backend.set("alice", "pw", "user", None).await;
        let s = store(backend);
        let resp = s
            .login(Creds {
                handle: "alice".into(),
                password: "pw".into(),
            })
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
            .login(Creds {
                handle: "alice".into(),
                password: "pw".into(),
            })
            .await
            .unwrap();
        assert!(s.validate_bearer(&resp.token).is_some());
        s.logout(Some(&resp.token), resp.refresh_token.as_deref())
            .await;
        assert!(s.validate_bearer(&resp.token).is_none());
    }

    // ---- the SessionStore seam + the absolute cap ----

    fn alice() -> Creds {
        Creds {
            handle: "alice".into(),
            password: "pw".into(),
        }
    }

    async fn alice_backend() -> TestBackend {
        let backend = TestBackend::default();
        backend.set("alice", "pw", "user", None).await;
        backend
    }

    /// A store over a shared [`MemorySessionStore`] the test can inspect.
    fn store_with(
        backend: TestBackend,
        mem: &Arc<MemorySessionStore>,
        refresh_ttl_secs: u64,
        max_session_secs: Option<u64>,
    ) -> CredentialStore<TestBackend> {
        CredentialStore::new_with_options(
            backend,
            "test-secret",
            SessionOptions {
                refresh_ttl_secs,
                max_session_secs,
                store: mem.clone(),
            },
        )
    }

    /// Moves every live session's original login `secs` into the past.
    fn backdate_login(mem: &MemorySessionStore, secs: u64) {
        for s in lock(&mem.refresh).values_mut() {
            s.login_at -= secs;
        }
    }

    #[tokio::test]
    async fn concurrent_refreshes_of_one_token_yield_one_winner() {
        let s = store(alice_backend().await);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        let (a, b) = tokio::join!(s.refresh(&rt), s.refresh(&rt));
        assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1, "exactly one may win");
    }

    #[tokio::test]
    async fn expired_refresh_token_is_refused_and_consumed() {
        let mem = Arc::new(MemorySessionStore::new());
        // TTL 0: the token's expiry is its issue second, i.e. already expired.
        let s = store_with(alice_backend().await, &mem, 0, None);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        assert!(matches!(
            s.refresh(&rt).await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        assert!(lock(&mem.refresh).is_empty(), "take-then-check consumes it");
    }

    #[tokio::test]
    async fn login_sweeps_expired_refresh_sessions() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 0, None);
        for _ in 0..3 {
            s.login(alice()).await.unwrap();
        }
        // Each login swept the previous (expired) entry before adding its own.
        assert_eq!(lock(&mem.refresh).len(), 1);
    }

    #[tokio::test]
    async fn logout_revokes_drops_the_refresh_token_and_sweeps() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 3600, None);
        mem.revoke_access("stale", 1, 1);
        let resp = s.login(alice()).await.unwrap();
        let rt = resp.refresh_token.unwrap();
        s.logout(Some(&resp.token), Some(&rt)).await;
        assert!(s.validate_bearer(&resp.token).is_none());
        assert!(s.refresh(&rt).await.is_err());
        let revoked = lock(&mem.revoked);
        assert!(!revoked.contains_key("stale"), "expired revocation swept");
        assert_eq!(revoked.len(), 1);
    }

    #[tokio::test]
    async fn revoke_refresh_token_alone_drops_it() {
        let s = store(alice_backend().await);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        s.revoke_refresh_token(&rt).await;
        assert!(s.refresh(&rt).await.is_err());
    }

    /// Records every key a store is handed.
    #[derive(Default)]
    struct RecordingStore {
        inner: MemorySessionStore,
        keys: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingStore {
        fn saw(&self, key: &str) {
            lock(&self.keys).push(key.to_owned());
        }
    }

    impl SessionStore for RecordingStore {
        fn put_refresh(&self, key: &str, session: RefreshSession, now: u64) {
            self.saw(key);
            self.inner.put_refresh(key, session, now)
        }
        fn take_refresh(&self, key: &str) -> Option<RefreshSession> {
            self.saw(key);
            self.inner.take_refresh(key)
        }
        fn remove_refresh(&self, key: &str) {
            self.saw(key);
            self.inner.remove_refresh(key)
        }
        fn revoke_access(&self, key: &str, expires_at: u64, now: u64) {
            self.saw(key);
            self.inner.revoke_access(key, expires_at, now)
        }
        fn is_revoked(&self, key: &str, now: u64) -> bool {
            self.saw(key);
            self.inner.is_revoked(key, now)
        }
    }

    #[tokio::test]
    async fn the_store_never_sees_a_raw_token() {
        let rec = Arc::new(RecordingStore::default());
        let s = CredentialStore::new_with_options(
            alice_backend().await,
            "test-secret",
            SessionOptions {
                store: rec.clone(),
                ..SessionOptions::default()
            },
        );
        let first = s.login(alice()).await.unwrap();
        let second = s
            .refresh(first.refresh_token.as_deref().unwrap())
            .await
            .unwrap();
        s.validate_bearer(&second.token).unwrap();
        s.logout(Some(&second.token), second.refresh_token.as_deref())
            .await;
        let raw = [
            first.token,
            first.refresh_token.unwrap(),
            second.token,
            second.refresh_token.unwrap(),
        ];
        let keys = lock(&rec.keys);
        assert!(keys.len() >= 5);
        for k in keys.iter() {
            assert_eq!(k.len(), 64, "{k}");
            assert!(k.bytes().all(|b| b.is_ascii_hexdigit()), "{k}");
            assert!(
                raw.iter()
                    .all(|t| !t.contains(k.as_str()) && !k.contains(t.as_str()))
            );
        }
        assert!(keys.contains(&session_key(&raw[1])));
    }

    #[tokio::test]
    async fn rotation_keeps_the_original_login_time() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 3600, Some(7200));
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 600);
        let login_at = lock(&mem.refresh).values().next().unwrap().login_at;
        let rt2 = s.refresh(&rt).await.unwrap().refresh_token.unwrap();
        s.refresh(&rt2).await.unwrap();
        let map = lock(&mem.refresh);
        assert_eq!(map.len(), 1);
        let entry = map.values().next().unwrap();
        assert_eq!(entry.login_at, login_at, "carried through two rotations");
        assert!(entry.expires_at >= now_secs() + 3599, "expiry still slides");
    }

    #[tokio::test]
    async fn refresh_past_the_session_cap_is_refused() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 30 * 86_400, Some(3600));
        // Within the cap: fine.
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 3000);
        let rt = s.refresh(&rt).await.unwrap().refresh_token.unwrap();
        // Past it (the token itself is fresh — only login_at is old): refused, and consumed.
        backdate_login(&mem, 700);
        assert!(matches!(
            s.refresh(&rt).await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        assert!(lock(&mem.refresh).is_empty());
    }

    // ---- fork ----

    #[tokio::test]
    async fn fork_spends_the_token_and_yields_two_independent_chains() {
        let s = store(alice_backend().await);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        let f = s.fork(&rt, "alice").await.unwrap();
        // The forked token is dead, for refresh and for fork.
        assert!(matches!(
            s.refresh(&rt).await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        assert!(s.fork(&rt, "alice").await.is_err());
        let o = f.opener.refresh_token.clone().unwrap();
        let c = f.child.refresh_token.clone().unwrap();
        assert_ne!(o, c);
        assert_ne!(f.opener.token, f.child.token);
        assert_eq!(f.child.handle, "alice");
        assert!(s.validate_bearer(&f.opener.token).is_some());
        assert!(s.validate_bearer(&f.child.token).is_some());
        // Rotating one chain leaves the other alive, twice over.
        let o2 = s.refresh(&o).await.unwrap().refresh_token.unwrap();
        let o3 = s.refresh(&o2).await.unwrap().refresh_token.unwrap();
        let c2 = s.refresh(&c).await.unwrap().refresh_token.unwrap();
        assert!(s.refresh(&o3).await.is_ok());
        assert!(s.refresh(&c2).await.is_ok());
        // Logging one out leaves the other alive.
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        let f2 = s.fork(&rt, "alice").await.unwrap();
        s.logout(Some(&f2.opener.token), f2.opener.refresh_token.as_deref())
            .await;
        assert!(s.validate_bearer(&f2.child.token).is_some());
        let child_rt = f2.child.refresh_token.as_deref().unwrap();
        assert!(s.refresh(child_rt).await.is_ok());
    }

    #[tokio::test]
    async fn fork_keeps_the_original_login_time_on_both() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 3600, Some(7200));
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 600);
        let login_at = lock(&mem.refresh).values().next().unwrap().login_at;
        let f = s.fork(&rt, "alice").await.unwrap();
        let c2 = s
            .refresh(f.child.refresh_token.as_deref().unwrap())
            .await
            .unwrap();
        let map = lock(&mem.refresh);
        assert_eq!(map.len(), 2);
        for k in [
            session_key(f.opener.refresh_token.as_deref().unwrap()),
            session_key(c2.refresh_token.as_deref().unwrap()),
        ] {
            assert_eq!(map[&k].login_at, login_at, "both chains keep the sign-in");
        }
    }

    #[tokio::test]
    async fn fork_honours_the_cap_on_both_chains_and_on_itself() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 30 * 86_400, Some(3600));
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 3000);
        let f = s.fork(&rt, "alice").await.unwrap();
        // The child chain hits the same cap as the opener's.
        backdate_login(&mem, 700);
        for t in [&f.opener, &f.child] {
            let rt = t.refresh_token.as_deref().unwrap();
            assert!(matches!(
                s.refresh(rt).await.unwrap_err(),
                IdentityError::InvalidRefreshToken
            ));
        }
        // A fork past the cap is refused (and consumed, as refresh does).
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 3601);
        assert!(matches!(
            s.fork(&rt, "alice").await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        assert!(lock(&mem.refresh).is_empty());
    }

    #[tokio::test]
    async fn fork_by_another_user_is_refused_and_leaves_the_token_alive() {
        let backend = alice_backend().await;
        backend.set("bob", "pw", "user", None).await;
        let s = store(backend);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        assert!(matches!(
            s.fork(&rt, "bob").await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        // Not consumed: alice can still refresh it.
        assert!(s.refresh(&rt).await.is_ok());
    }

    #[tokio::test]
    async fn the_default_take_refresh_for_puts_a_mismatch_back() {
        // RecordingStore does not override take_refresh_for.
        let rec = Arc::new(RecordingStore::default());
        let s = CredentialStore::new_with_options(
            alice_backend().await,
            "test-secret",
            SessionOptions {
                store: rec.clone(),
                ..SessionOptions::default()
            },
        );
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        assert!(s.fork(&rt, "bob").await.is_err());
        assert!(s.fork(&rt, "alice").await.is_ok());
    }

    #[tokio::test]
    async fn fork_of_an_unknown_or_expired_token_is_a_plain_reject() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 0, None);
        assert!(matches!(
            s.fork("no-such-token", "alice").await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        assert!(matches!(
            s.fork(&rt, "alice").await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
        // A plain refresh reject is unchanged.
        assert!(matches!(
            s.refresh("no-such-token").await.unwrap_err(),
            IdentityError::InvalidRefreshToken
        ));
    }

    #[tokio::test]
    async fn concurrent_forks_of_one_token_yield_one_winner() {
        let s = store(alice_backend().await);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        let (a, b) = tokio::join!(s.fork(&rt, "alice"), s.refresh(&rt));
        assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1, "exactly one may win");
    }

    #[tokio::test]
    async fn no_cap_by_default() {
        let mem = Arc::new(MemorySessionStore::new());
        let s = store_with(alice_backend().await, &mem, 3600, None);
        let rt = s.login(alice()).await.unwrap().refresh_token.unwrap();
        backdate_login(&mem, 10 * 365 * 86_400);
        assert!(s.refresh(&rt).await.is_ok());
    }
}
