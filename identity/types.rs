use serde::{Deserialize, Serialize};

/// Request payload for creating a new identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Unique user handle to register.
    pub handle: String,
    /// Plain-text password to hash and store.
    pub password: String,
    /// Role assigned to the new user.
    pub role: String,
}

/// Request payload for logging an existing user in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    /// Registered user handle.
    pub handle: String,
    /// Plain-text password to verify.
    pub password: String,
}

/// Request payload for validating a previously issued JWT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateRequest {
    /// Access token to validate.
    pub token: String,
}

/// Request payload for exchanging a refresh token for a fresh token pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshRequest {
    /// Refresh token to rotate.
    pub refresh_token: String,
}

/// Request payload for invalidating one or more tokens (logout).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutRequest {
    /// The current access token to revoke, if available.
    #[serde(default)]
    pub token: Option<String>,
    /// The refresh token to revoke, if available.
    #[serde(default)]
    pub refresh_token: Option<String>,
}

/// Response payload containing a fresh access/refresh token pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    /// Encoded access token.
    pub token: String,
    /// Encoded refresh token.
    pub refresh_token: String,
    /// Authenticated user handle.
    pub handle: String,
    /// Role assigned to the user.
    pub role: String,
}

/// Response payload for a successful logout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutResponse {}

/// Response payload containing the validated caller role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleResponse {
    /// Role embedded in the validated JWT.
    pub role: String,
}

/// Errors returned by the identity service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IdentityError {
    /// The requested handle already exists.
    HandleAlreadyTaken,
    /// The provided login credentials did not match a stored user.
    InvalidCredentials,
    /// The provided access token could not be validated.
    InvalidToken,
    /// The provided refresh token could not be validated.
    InvalidRefreshToken,
    /// An internal dependency failed and produced a message.
    Internal(String),
}
