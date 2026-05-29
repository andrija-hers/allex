mod types;
pub use types::*;
mod store;
pub use store::*;

use rpc_macro::rpc_module;

/// The identity service: register users, issue JWTs, and validate them.
#[rpc_module]
pub trait IdentityV1 {
    #[public]
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    #[public]
    fn login(
        &self,
        req: LoginRequest,
    ) -> impl std::future::Future<Output = Result<TokenResponse, IdentityError>> + Send;

    #[public]
    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl std::future::Future<Output = Result<RoleResponse, IdentityError>> + Send;
}
