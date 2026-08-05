//! Account management and out-of-band session minting for identity stores.
//!
//! [`IdentityV1`](crate::IdentityV1) is the *login* surface (register / login / refresh / validate /
//! logout). [`AccountStore`] is the *management* surface a service needs around it: check a
//! password (step-up), change a password, read a user, list every user, change a role, delete an
//! account, mint a session for a user who was authenticated **out of band** (passkey / SSO / a
//! custom login), and revoke sessions.
//!
//! Together those are enough to build an admin user-management screen without a store ever needing
//! to expose self-registration — see [`RegistrationGuard`](crate::RegistrationGuard), which closes
//! public signup while leaving this surface open to callers the *service* has authorized.
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

    /// Every account in the store, ordered by handle. The building block for an admin user list.
    ///
    /// Deliberately unpaginated: this is the *management* surface, sized for the tens-to-hundreds
    /// of accounts an operations team curates by hand, not for a consumer signup table. A store
    /// with millions of users wants its own paged query rather than this.
    fn list_accounts(
        &self,
    ) -> impl Future<Output = Result<Vec<AccountRecord>, IdentityError>> + Send;

    /// Replaces `handle`'s role, and **revokes the user's refresh tokens** as part of the same
    /// operation. Errors [`IdentityError::UserNotFound`] if the handle is unknown.
    ///
    /// The revocation is not optional politeness, which is why this differs from
    /// [`set_password`](AccountStore::set_password). A persisted refresh token carries the role it
    /// was minted with, so a demoted user holding one could refresh straight back into the role
    /// they just lost — for as long as that token lives. Dropping the tokens closes it.
    ///
    /// What it cannot close: an **access token already in the user's hands** still carries the old
    /// role until it expires, because a JWT is verified by signature, not by a lookup. Between a
    /// demotion and that expiry the user keeps their old powers. Keep the access-token TTL short if
    /// that window matters — and do not treat this call as an instant lockout. For that, delete the
    /// account.
    fn set_role(
        &self,
        handle: &str,
        role: &str,
    ) -> impl Future<Output = Result<(), IdentityError>> + Send;

    /// Deletes the account **and every refresh token it owns**. Errors
    /// [`IdentityError::UserNotFound`] if the handle is unknown, so a caller can tell a real
    /// deletion from a no-op.
    ///
    /// The token cleanup is done explicitly rather than left to a schema's `ON DELETE CASCADE`: a
    /// store whose cascade is not actually enforced at runtime would strand live refresh tokens
    /// pointing at a deleted handle, and a refresh reads that row without re-checking that the user
    /// still exists — a deleted account that can still mint sessions. The same access-token window
    /// as [`set_role`](AccountStore::set_role) applies here too.
    fn delete_account(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<(), IdentityError>> + Send;

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
