//! Identity service contracts and an in-memory reference implementation.

// The in-memory store holds a few intentionally-nested `Arc<Mutex<HashMap<..>>>` maps.
#![allow(clippy::type_complexity)]

mod types;
pub use types::*;
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
