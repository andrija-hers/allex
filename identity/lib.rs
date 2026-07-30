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
//! [`IdentityV1`] is `register` / `login` / `refresh` / `validate` / `logout` /
//! `revoke_refresh_token`. It's a normal `#[rpc_module]` trait, so it's registered and served
//! exactly like any other module.
//!
//! The access tiers say who may call what, and the split at the end is deliberate:
//!
//! | method | tier | why |
//! |---|---|---|
//! | `register`, `login`, `refresh`, `validate` | `#[public]` | this is how a caller *gets* an identity |
//! | `logout` | `#[authenticated]` | ends the session you are calling with |
//! | `revoke_refresh_token` | `#[public]` | possession of the token is the proof, and the session it belongs to may already be dead |
//!
//! **`logout` carried no tier at all until 2026-07**, which the generated permission table read as
//! *nobody may call this* — so every logout over RPC answered 403 and no session was ever ended.
//! The tier it needed did not exist yet. Adding `#[authenticated]` fixed the method; making an
//! unannotated method a compile error fixed the class.
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
mod credential_store;
/// Reusable JWT engine (issue/validate access & refresh tokens, optional opaque
/// per-user profile claims). Previously an orphan file; wired in so custom
/// stores can share it instead of each re-deriving JWT plumbing.
pub mod tokens;
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

    /// Ends the session this call is made with: the access token is refused from
    /// now on, and the paired refresh token is deleted when one is supplied.
    ///
    /// The access token has to travel in the request body because a handler
    /// cannot see the bearer — `CallerContext` carries the handle, role and
    /// profile, not the credential.
    ///
    /// A refresh token belonging to *another* account is refused: this method is
    /// "end my session", not "end any session whose token I have obtained".
    /// Ending a session whose access token has already expired is
    /// [`Self::revoke_refresh_token`], which needs no live session.
    ///
    /// **Historical note.** Until 2026-07 this method carried no access-tier
    /// attribute, which the generated permission table read as *reachable by
    /// nobody*: every caller got a 403, so no session was ever actually ended
    /// over RPC. The tier it needed — `#[authenticated]` — did not exist. Both
    /// halves of that are fixed; an unannotated method is now a compile error.
    #[authenticated]
    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl std::future::Future<Output = Result<LogoutResponse, IdentityError>> + Send;

    /// Invalidates a refresh token, with no live session required.
    ///
    /// `#[public]` because possession of the token *is* the proof, exactly as it
    /// is for a password-reset link — and because the case this exists for is a
    /// session that can no longer authenticate: the access token has expired, the
    /// app cold-started, or a queued sign-out is being retried. Requiring
    /// authentication there would mean the credential that outlives the session
    /// is the one you cannot revoke.
    ///
    /// **Non-enumerating.** Unknown, expired and already-revoked tokens all
    /// answer with the same empty success. Reporting "no such token" would make
    /// this an oracle for whether a token is live, callable by anyone.
    ///
    /// Possession already permits minting access tokens, so exposing this adds
    /// nuisance (someone with a leaked token can end that session) and no
    /// privilege. Rate limiting is the host's business.
    #[public]
    fn revoke_refresh_token(
        &self,
        req: RevokeRefreshTokenRequest,
    ) -> impl std::future::Future<Output = Result<RevokeRefreshTokenResponse, IdentityError>> + Send;
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

/// What the generated permission table says about each of `IdentityV1`'s
/// methods, asserted at the layer where the answer is actually produced.
///
/// This is the test that was missing. Every store's `logout` test calls the
/// store *directly*, so all of them passed while the method was unreachable over
/// RPC: the authorization gate lives in the generated responder, and nothing
/// exercised it. A dead endpoint is invisible from below.
#[cfg(test)]
mod access_tier_tests {
    use super::IdentityV1MethodPermissions as P;

    /// The wire form of a caller: `handle\x1erole`. Passing a bare role would
    /// test something the server never sees.
    fn caller(role: &str) -> String {
        format!("andra\u{1e}{role}")
    }

    #[test]
    fn logout_is_callable_by_any_authenticated_caller() {
        // The regression this file exists for. Before `#[authenticated]`, every
        // one of these was false and `logout` answered 403 to the whole world.
        for role in [
            "user",
            "admin",
            "moderator",
            "viewer",
            "whatever-a-host-invents",
        ] {
            assert!(
                P::is_authorized("logout", Some(&caller(role))),
                "an authenticated {role} must be able to end their own session"
            );
        }
    }

    #[test]
    fn logout_still_refuses_anonymous_callers() {
        // `#[authenticated]` is not `#[public]`: no identity, no logout.
        assert!(!P::is_authorized("logout", None));
        assert!(P::requires_authentication("logout"));
        assert!(!P::is_public("logout"));
        // And it is not role-restricted — having an identity is the whole test.
        assert!(P::allowed_roles("logout").is_none());
    }

    #[test]
    fn revoking_a_refresh_token_needs_no_session() {
        // The half of logout that must work when the session is already dead.
        assert!(P::is_authorized("revoke_refresh_token", None));
        assert!(P::is_public("revoke_refresh_token"));
        assert!(!P::requires_authentication("revoke_refresh_token"));
    }

    #[test]
    fn the_credential_endpoints_stay_public() {
        for method in ["register", "login", "refresh", "validate"] {
            assert!(
                P::is_authorized(method, None),
                "{method} is how a caller gets an identity in the first place"
            );
        }
    }

    #[test]
    fn an_unknown_method_is_authorized_for_nobody() {
        // The fallback arm: a method that does not exist must not be reachable,
        // which is what makes the permission gate safe to run before dispatch.
        assert!(!P::is_authorized("no_such_method", None));
        assert!(!P::is_authorized("no_such_method", Some(&caller("admin"))));
    }
}
