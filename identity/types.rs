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
    /// Bearer token to validate.
    pub token: String,
}

/// Request payload for invalidating a JWT (logout).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutRequest {
    /// The bearer token to revoke.
    pub token: String,
}

/// Response payload containing a newly issued JWT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    /// Encoded JWT.
    pub token: String,
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
    /// The provided token could not be validated.
    InvalidToken,
    /// An internal dependency failed and produced a message.
    Internal(String),
}