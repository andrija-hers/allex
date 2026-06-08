use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::{IdentityError, TokenResponse};

pub const ACCESS_TOKEN_TTL_SECS: u64 = 15 * 60;
pub const REFRESH_TOKEN_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TokenKind {
    Access,
    Refresh,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
    kind: TokenKind,
    nonce: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedToken {
    pub handle: String,
    pub role: String,
    pub expires_at_unix: u64,
    pub refresh_session_id: Option<String>,
}

pub fn issue_token_response(
    secret: &str,
    handle: &str,
    role: &str,
) -> Result<TokenResponse, IdentityError> {
    Ok(TokenResponse {
        token: issue_access_token(secret, handle, role)?,
        refresh_token: issue_refresh_token(secret, handle, role)?,
        handle: handle.to_owned(),
        role: role.to_owned(),
    })
}

pub fn issue_token_response_with_refresh_session(
    secret: &str,
    handle: &str,
    role: &str,
    refresh_session_id: &str,
) -> Result<TokenResponse, IdentityError> {
    Ok(TokenResponse {
        token: issue_access_token(secret, handle, role)?,
        refresh_token: issue_refresh_token_with_session(secret, handle, role, refresh_session_id)?,
        handle: handle.to_owned(),
        role: role.to_owned(),
    })
}

pub fn issue_access_token(secret: &str, handle: &str, role: &str) -> Result<String, IdentityError> {
    issue_token(secret, handle, role, TokenKind::Access, ACCESS_TOKEN_TTL_SECS, None)
}

pub fn issue_refresh_token(
    secret: &str,
    handle: &str,
    role: &str,
) -> Result<String, IdentityError> {
    issue_token(secret, handle, role, TokenKind::Refresh, REFRESH_TOKEN_TTL_SECS, None)
}

pub fn issue_refresh_token_with_session(
    secret: &str,
    handle: &str,
    role: &str,
    refresh_session_id: &str,
) -> Result<String, IdentityError> {
    issue_token(
        secret,
        handle,
        role,
        TokenKind::Refresh,
        REFRESH_TOKEN_TTL_SECS,
        Some(refresh_session_id),
    )
}

pub fn validate_access_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Access)
}

pub fn validate_refresh_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Refresh)
}

fn issue_token(
    secret: &str,
    handle: &str,
    role: &str,
    kind: TokenKind,
    ttl_secs: u64,
    refresh_session_id: Option<&str>,
) -> Result<String, IdentityError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let claims = Claims {
        sub: handle.to_owned(),
        role: role.to_owned(),
        exp: (now.as_secs() + ttl_secs) as usize,
        kind,
        nonce: now.as_nanos().to_string(),
        refresh_session_id: refresh_session_id.map(str::to_owned),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| IdentityError::Internal(e.to_string()))
}

fn decode_token(secret: &str, token: &str, expected_kind: TokenKind) -> Result<ValidatedToken, IdentityError> {
    let claims = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| invalid_token_error(expected_kind))?
    .claims;

    if claims.kind != expected_kind {
        return Err(invalid_token_error(expected_kind));
    }

    Ok(ValidatedToken {
        handle: claims.sub,
        role: claims.role,
        expires_at_unix: claims.exp as u64,
        refresh_session_id: claims.refresh_session_id,
    })
}

fn invalid_token_error(kind: TokenKind) -> IdentityError {
    match kind {
        TokenKind::Access => IdentityError::InvalidToken,
        TokenKind::Refresh => IdentityError::InvalidRefreshToken,
    }
}
