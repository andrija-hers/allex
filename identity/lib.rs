//! Identity service contracts and an in-memory reference implementation.

mod types;
pub use types::*;
mod tokens;
pub use tokens::*;
mod store;
pub use store::*;

use rpc_macro::rpc_module;

/// The identity service: register users, issue access/refresh token pairs,
/// validate access tokens, rotate refresh tokens, and revoke credentials.
#[rpc_module]
pub trait IdentityV1 {
    /// Registers a new user, then returns fresh access and refresh tokens.
    #[public]
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Authenticates an existing user and returns fresh access and refresh tokens.
    #[public]
    fn login(
        &self,
        req: LoginRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Exchanges a valid refresh token for a rotated access/refresh token pair.
    #[public]
    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    /// Validates an access token and returns the caller role encoded in that token.
    #[public]
    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl std::future::Future<Output = Result<RoleResponse, IdentityError>> + Send;

    /// Revokes the provided access and/or refresh tokens so they are rejected
    /// on all subsequent requests.
    #[public]
    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl std::future::Future<Output = Result<LogoutResponse, IdentityError>> + Send;
}
