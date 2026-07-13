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
    /// Opaque, project-defined claims baked into the token (e.g. a JSON string
    /// of per-user rights flags). Absent from tokens issued without a profile,
    /// so older tokens keep decoding unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedToken {
    pub handle: String,
    pub role: String,
    pub expires_at_unix: u64,
    pub refresh_session_id: Option<String>,
    /// The opaque project-defined profile carried in the token, if any (see
    /// [`issue_access_token_with_profile`]). Kept as an opaque `String` — decode
    /// it into your own type at the project boundary.
    pub profile: Option<String>,
}

pub fn issue_token_response(
    secret: &str,
    handle: &str,
    role: &str,
) -> Result<TokenResponse, IdentityError> {
    Ok(TokenResponse {
        token: issue_access_token(secret, handle, role)?,
        refresh_token: Some(issue_refresh_token(secret, handle, role)?),
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
        refresh_token: Some(issue_refresh_token_with_session(
            secret,
            handle,
            role,
            refresh_session_id,
        )?),
        handle: handle.to_owned(),
        role: role.to_owned(),
    })
}

pub fn issue_access_token(secret: &str, handle: &str, role: &str) -> Result<String, IdentityError> {
    issue_token(
        secret,
        handle,
        role,
        TokenKind::Access,
        ACCESS_TOKEN_TTL_SECS,
        None,
        None,
    )
}

/// Issues an access token that additionally carries an opaque, project-defined
/// `profile` payload (e.g. a JSON string of per-user rights flags).
///
/// A store re-emits this profile as the third `\x1e`-separated segment of its
/// `validate_bearer` result, so it reaches `allex_serve::caller_profile()`. The
/// profile is **stateless** — frozen into the token at issue time — so
/// re-resolve it whenever you mint a fresh token (login and refresh) if you want
/// rights changes to propagate.
pub fn issue_access_token_with_profile(
    secret: &str,
    handle: &str,
    role: &str,
    profile: Option<&str>,
) -> Result<String, IdentityError> {
    issue_token(
        secret,
        handle,
        role,
        TokenKind::Access,
        ACCESS_TOKEN_TTL_SECS,
        None,
        profile,
    )
}

/// Like [`issue_token_response`] but bakes an opaque `profile` payload into the
/// access token (see [`issue_access_token_with_profile`]).
pub fn issue_token_response_with_profile(
    secret: &str,
    handle: &str,
    role: &str,
    profile: Option<&str>,
) -> Result<TokenResponse, IdentityError> {
    Ok(TokenResponse {
        token: issue_access_token_with_profile(secret, handle, role, profile)?,
        refresh_token: Some(issue_refresh_token(secret, handle, role)?),
        handle: handle.to_owned(),
        role: role.to_owned(),
    })
}

pub fn issue_refresh_token(
    secret: &str,
    handle: &str,
    role: &str,
) -> Result<String, IdentityError> {
    issue_token(
        secret,
        handle,
        role,
        TokenKind::Refresh,
        REFRESH_TOKEN_TTL_SECS,
        None,
        None,
    )
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
        None,
    )
}

pub fn validate_access_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Access)
}

pub fn validate_refresh_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Refresh)
}

#[allow(clippy::too_many_arguments)]
fn issue_token(
    secret: &str,
    handle: &str,
    role: &str,
    kind: TokenKind,
    ttl_secs: u64,
    refresh_session_id: Option<&str>,
    profile: Option<&str>,
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
        profile: profile.map(str::to_owned),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| IdentityError::Internal(e.to_string()))
}

fn decode_token(
    secret: &str,
    token: &str,
    expected_kind: TokenKind,
) -> Result<ValidatedToken, IdentityError> {
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
        profile: claims.profile,
    })
}

fn invalid_token_error(kind: TokenKind) -> IdentityError {
    match kind {
        TokenKind::Access => IdentityError::InvalidToken,
        TokenKind::Refresh => IdentityError::InvalidRefreshToken,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-secret";

    #[test]
    fn access_token_without_profile_validates_with_none() {
        let token = issue_access_token(SECRET, "alice", "operator").unwrap();
        let v = validate_access_token(SECRET, &token).unwrap();
        assert_eq!(v.handle, "alice");
        assert_eq!(v.role, "operator");
        assert_eq!(v.profile, None);
    }

    #[test]
    fn access_token_profile_round_trips_opaquely() {
        let profile = r#"{"r_admin":true,"r_backdate":false}"#;
        let token =
            issue_access_token_with_profile(SECRET, "alice", "user", Some(profile)).unwrap();
        let v = validate_access_token(SECRET, &token).unwrap();
        assert_eq!(v.handle, "alice");
        assert_eq!(v.role, "user");
        // Carried verbatim; the token layer never interprets it.
        assert_eq!(v.profile.as_deref(), Some(profile));
    }

    #[test]
    fn token_response_with_profile_puts_profile_in_access_not_refresh() {
        let profile = r#"{"r_admin":true}"#;
        let resp = issue_token_response_with_profile(SECRET, "bob", "user", Some(profile)).unwrap();
        assert_eq!(
            validate_access_token(SECRET, &resp.token)
                .unwrap()
                .profile
                .as_deref(),
            Some(profile)
        );
        // The refresh token intentionally carries no profile — re-resolve on refresh.
        let rt = resp.refresh_token.unwrap();
        assert_eq!(validate_refresh_token(SECRET, &rt).unwrap().profile, None);
    }
}
