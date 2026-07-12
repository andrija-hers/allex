//! Authentication for allex: the [`IdentityV1`] service, ready-made stores, and the seam for
//! building your own without re-deriving JWT plumbing.
//!
//! # The one decision: which entry point — and *don't hand-write a store*
//!
//! The mistake this crate exists to prevent is re-implementing the whole [`IdentityV1`] +
//! `BearerTokenValidator` surface for every project. There are **three layers**, and you pick the
//! highest one that fits:
//!
//! 1. **A stock store** — [`InMemoryIdentityStore`] here (dev/tests), plus `SqliteIdentityStore`,
//!    `PostgresIdentityStore`, `RedbIdentityStore` in sibling crates. Use one directly when your
//!    users are `{handle, password}` + a single role, bcrypt-hashed. Seed the first account with
//!    [`ensure_user`] (idempotent — it swallows `HandleAlreadyTaken`).
//! 2. **[`CredentialStore<B>`]** — when your credentials or rights don't fit that shape (external
//!    DB, SSO, a stored proc, per-user permission flags). Implement [`CredentialBackend`] — just
//!    [`verify_credentials`](CredentialBackend::verify_credentials) and
//!    [`resolve_user`](CredentialBackend::resolve_user) — and wrap it; `CredentialStore` provides
//!    `login`/`refresh`/`logout` **and** `BearerTokenValidator` for free, over the shared JWT
//!    engine. It carries an opaque per-user **profile** in the token. **Do not write a fifth
//!    store by hand.**
//! 3. **[`tokens`]** — the raw JWT engine (issue/validate access & refresh tokens, optional opaque
//!    profile claim). Only reach here if you're building a store from scratch.
//!
//! # The service
//!
//! [`IdentityV1`] is `register` / `login` / `refresh` / `validate` / `logout`; each returns a
//! `TokenResponse` (an access + refresh JWT pair). All are `#[public]` except `logout`. It's a
//! normal `#[rpc_module]` trait, so it's registered and served exactly like any other module.
//!
//! # How it plugs into the rest of allex
//!
//! Every store and every [`CredentialStore`] implements `transport::BearerTokenValidator`, so you
//! hand one to `AxumTransport::router_with_auth(..)` and the caller identity flows to your methods
//! as `allex_serve::caller_handle()` / `caller_role()` / `caller_profile()`. **The `profile` is an
//! opaque `String` on purpose — allex never parses it.** The design rules around profile
//! freshness, size, and the `role`-vs-`profile` split are load-bearing and easy to get wrong; the
//! `credential_store` module `//!` and the **Identity** section of the workspace `AGENTS.md` are the
//! canonical guide — read them before choosing profile-based authorization.
//!
//! ```
//! use allex_identity::tokens::{issue_access_token, validate_access_token};
//!
//! // The JWT engine (layer 3) is pure and synchronous — no store, no runtime.
//! let secret = "server-secret";
//! let token = issue_access_token(secret, "alice", "admin").unwrap();
//!
//! let validated = validate_access_token(secret, &token).unwrap();
//! assert_eq!(validated.handle, "alice");
//! assert_eq!(validated.role, "admin");
//! assert_eq!(validated.profile, None); // issued without a profile claim
//! ```

// The in-memory store holds a few intentionally-nested `Arc<Mutex<HashMap<..>>>` maps.
#![allow(clippy::type_complexity)]

mod types;
pub use types::*;
mod account;
pub use account::*;
mod registration_guard;
pub use registration_guard::*;
mod store;
pub use store::*;
/// Reusable JWT engine (issue/validate access & refresh tokens, optional opaque
/// per-user profile claims). Previously an orphan file; wired in so custom
/// stores can share it instead of each re-deriving JWT plumbing.
pub mod tokens;
mod credential_store;
pub use credential_store::*;

use rpc_macro::rpc_module;

/// The identity service: register users, issue JWTs, and validate them.
#[rpc_module]
pub trait IdentityV1 {
    /// Registers a new user, then returns a freshly issued token pair.
    #[public]
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Authenticates an existing user and returns a fresh token pair on success.
    #[public]
    fn login(
        &self,
        req: LoginRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Exchanges a valid refresh token for a new token pair.
    #[public]
    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Validates a JWT and returns the caller role encoded in that token.
    #[public]
    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl std::future::Future<Output = Result<RoleResponse, IdentityError>> + Send;

    /// Revokes an access and/or refresh token so they are rejected on all subsequent requests.
    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl std::future::Future<Output = Result<LogoutResponse, IdentityError>> + Send;
}

/// Idempotently ensures a user exists by registering `(handle, password, role)`
/// and treating [`IdentityError::HandleAlreadyTaken`] as success.
///
/// Use it to seed a service's initial operator account on startup without
/// clobbering an already-seeded one — the pattern every allex host otherwise
/// hand-writes around [`IdentityV1::register`].
pub async fn ensure_user<I>(
    identity: &I,
    handle: impl Into<String>,
    password: impl Into<String>,
    role: impl Into<String>,
) -> Result<(), IdentityError>
where
    I: IdentityV1,
{
    match identity
        .register(RegisterRequest {
            handle: handle.into(),
            password: password.into(),
            role: role.into(),
        })
        .await
    {
        Ok(_) | Err(IdentityError::HandleAlreadyTaken) => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod ensure_user_tests {
    use super::*;

    #[tokio::test]
    async fn ensure_user_is_idempotent() {
        let store = InMemoryIdentityStore::new("secret");
        // First call registers, second call must not error on the duplicate.
        ensure_user(&store, "operator", "pw", "operator")
            .await
            .unwrap();
        ensure_user(&store, "operator", "pw", "operator")
            .await
            .unwrap();
        // The seeded account logs in.
        assert!(
            store
                .login(LoginRequest {
                    handle: "operator".into(),
                    password: "pw".into(),
                })
                .await
                .is_ok()
        );
    }
}
