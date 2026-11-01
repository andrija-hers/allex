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
    /// The id of the sign-in session an access token belongs to (one per login, kept through every
    /// refresh, a new one per fork). Lets a store refuse every access token of a session the moment
    /// it ends. Absent from tokens issued without one, so older tokens keep decoding unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sid: Option<String>,
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
    /// The sign-in session the token belongs to, if it was issued with one (see
    /// [`issue_access_token_for_session`]).
    pub session_id: Option<String>,
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
        None,
    )
}

/// Like [`issue_access_token_with_profile`], and the token names the sign-in session `sid` it
/// belongs to (re-emitted as [`ValidatedToken::session_id`]), so a store can refuse it as soon as
/// that session ends.
pub fn issue_access_token_for_session(
    secret: &str,
    handle: &str,
    role: &str,
    profile: Option<&str>,
    sid: &str,
) -> Result<String, IdentityError> {
    issue_token(
        secret,
        handle,
        role,
        TokenKind::Access,
        ACCESS_TOKEN_TTL_SECS,
        None,
        profile,
        Some(sid),
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
        None,
    )
}

pub fn validate_access_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Access)
}

pub fn validate_refresh_token(secret: &str, token: &str) -> Result<ValidatedToken, IdentityError> {
    decode_token(secret, token, TokenKind::Refresh)
}

/// The session id of a genuine access token (signature and kind checked), **expired or not**: a
/// logout presenting a just-expired access token still names the session to end. `None` for a
/// forged token, a refresh token, or a token issued without a session id.
pub fn access_token_session_id(secret: &str, token: &str) -> Option<String> {
    let mut validation = Validation::default();
    validation.validate_exp = false;
    let claims = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()?
    .claims;
    (claims.kind == TokenKind::Access)
        .then_some(claims.sid)
        .flatten()
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
    sid: Option<&str>,
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
        sid: sid.map(str::to_owned),
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
        session_id: claims.sid,
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

    #[test]
    fn a_session_id_round_trips_and_is_readable_past_expiry() {
        let token = issue_access_token_for_session(SECRET, "alice", "user", None, "sid-1").unwrap();
        let v = validate_access_token(SECRET, &token).unwrap();
        assert_eq!(v.session_id.as_deref(), Some("sid-1"));
        assert_eq!(
            access_token_session_id(SECRET, &token).as_deref(),
            Some("sid-1")
        );
        // Expired: refused as a bearer, but its session is still named (for a logout).
        let expired = encode(
            &Header::default(),
            &Claims {
                sub: "alice".into(),
                role: "user".into(),
                exp: 1,
                kind: TokenKind::Access,
                nonce: "n".into(),
                refresh_session_id: None,
                profile: None,
                sid: Some("sid-2".into()),
            },
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        assert!(validate_access_token(SECRET, &expired).is_err());
        assert_eq!(
            access_token_session_id(SECRET, &expired).as_deref(),
            Some("sid-2")
        );
        // Forged, a refresh token, a token without one: none.
        assert_eq!(access_token_session_id("other-secret", &token), None);
        let rt = issue_refresh_token(SECRET, "alice", "user").unwrap();
        assert_eq!(access_token_session_id(SECRET, &rt), None);
        let plain = issue_access_token(SECRET, "alice", "user").unwrap();
        assert_eq!(access_token_session_id(SECRET, &plain), None);
        assert_eq!(
            validate_access_token(SECRET, &plain).unwrap().session_id,
            None
        );
    }
}
