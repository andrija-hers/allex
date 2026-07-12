//! Account management and out-of-band session minting for identity stores.
//!
//! [`IdentityV1`](crate::IdentityV1) is the *login* surface (register / login / refresh / validate /
//! logout). [`AccountStore`] is the *management* surface a service needs around it: check a
//! password (step-up), change a password, read a user, mint a session for a user who was
//! authenticated **out of band** (passkey / SSO / a custom login), and revoke sessions.
//!
//! Every stock allex store implements this over the **same** `users` / `refresh_tokens` schema and
//! the same bcrypt + token internals it uses for `login`/`refresh`. That is the whole point: custom
//! auth flows should go through this API instead of reaching into a store's private tables with raw
//! SQL and hand-minted JWTs — code that silently breaks the day the store changes its schema, its
//! bcrypt cost, or its token format.

use std::future::Future;

use crate::{IdentityError, TokenResponse};

/// A user's public account record — no secrets — as surfaced by [`AccountStore::get_account`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRecord {
    /// The user's handle (username).
    pub handle: String,
    /// The user's coarse role (the `#[allowed_roles(..)]` gate value).
    pub role: String,
}

/// Account management + out-of-band session minting, implemented by every stock identity store
/// alongside [`IdentityV1`](crate::IdentityV1).
///
/// Implementations reuse the store's own password hashing, token issuance, and refresh-token
/// persistence, so a session minted here is indistinguishable from one `login` returns and works
/// unchanged with [`IdentityV1::refresh`](crate::IdentityV1::refresh) and
/// [`IdentityV1::logout`](crate::IdentityV1::logout).
pub trait AccountStore {
    /// Returns `Ok(true)` iff `password` matches the stored hash for `handle`. Returns `Ok(false)`
    /// for a wrong password **or** an unknown handle — existence is never leaked. `Err` is reserved
    /// for a backend fault. Use for step-up auth and "confirm current password" checks.
    fn verify_password(
        &self,
        handle: &str,
        password: &str,
    ) -> impl Future<Output = Result<bool, IdentityError>> + Send;

    /// Replaces `handle`'s password with a fresh hash. Errors [`IdentityError::UserNotFound`] if the
    /// handle is unknown. This does **not** revoke existing sessions — pair it with
    /// [`revoke_sessions`](AccountStore::revoke_sessions) if a password change should sign other
    /// devices out.
    fn set_password(
        &self,
        handle: &str,
        new_password: &str,
    ) -> impl Future<Output = Result<(), IdentityError>> + Send;

    /// Reads a user's public [`AccountRecord`], or `None` if the handle is unknown.
    fn get_account(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<Option<AccountRecord>, IdentityError>> + Send;

    /// Issues a fresh access + refresh session for an **already-authenticated** user — the same
    /// persisted token pair `login` returns — **without** a password. For passkey / SSO / custom
    /// logins that verified the user out of band. The role is re-read from the store, and the
    /// refresh token is persisted so `refresh`/`logout` work unchanged. Errors
    /// [`IdentityError::UserNotFound`] if the handle does not exist.
    fn issue_session(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Revokes **every** refresh token for `handle` (a password change, or "log out everywhere").
    /// Access tokens already issued stay valid until they expire — this only stops future refreshes;
    /// shrink the access-token TTL if you need a faster cutoff. A no-op (still `Ok`) if the user has
    /// no sessions.
    fn revoke_sessions(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<(), IdentityError>> + Send;
}
