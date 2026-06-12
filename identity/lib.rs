//! Identity service contracts and an in-memory reference implementation.

mod types;
pub use types::*;
mod store;
pub use store::*;

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
