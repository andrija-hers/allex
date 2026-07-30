use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub handle: String,
    pub password: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub handle: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutRequest {
    pub token: Option<String>,
    pub refresh_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub token: String,
    pub refresh_token: Option<String>,
    pub handle: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutResponse {}

/// The refresh token to invalidate. Required, unlike the optional fields on
/// [`LogoutRequest`]: this call does one thing, so the thing it needs is not
/// optional.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeRefreshTokenRequest {
    pub refresh_token: String,
}

/// Deliberately empty. Saying whether the token existed would turn a public
/// method into an oracle for which refresh tokens are live — see
/// [`IdentityV1::revoke_refresh_token`](crate::IdentityV1::revoke_refresh_token).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeRefreshTokenResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleResponse {
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IdentityError {
    HandleAlreadyTaken,
    InvalidCredentials,
    InvalidToken,
    InvalidRefreshToken,
    /// The named user does not exist. Returned by account-management operations
    /// ([`crate::AccountStore`]) that target a specific handle; distinct from
    /// `InvalidCredentials`, which login uses so it never leaks account existence.
    UserNotFound,
    Internal(String),
}
