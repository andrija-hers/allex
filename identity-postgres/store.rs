use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
};

use allex_postgres::Db;
use bcrypt::{DEFAULT_COST, hash, verify};
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RefreshRequest,
    RegisterRequest, RoleResponse, TokenResponse, ValidateRequest,
    issue_token_response_with_refresh_session, validate_access_token, validate_refresh_token,
};
use sha2::{Digest, Sha256};
use transport::BearerTokenValidator;
use uuid::Uuid;

/// A PostgreSQL-backed identity store.
///
/// Persists users in a `users` table and refresh sessions in a durable
/// `refresh_sessions` table. Access-token logout remains an in-memory blacklist,
/// while refresh-token rotation and revocation survive process restarts.
#[derive(Clone)]
pub struct PostgresIdentityStore {
    db: Db,
    secret: String,
    _token_ttl_secs: u64,
    /// In-memory blacklist of revoked access tokens. Cleared on restart; access
    /// tokens are intentionally short-lived, so durable storage is reserved for
    /// refresh sessions.
    revoked_access_tokens: Arc<Mutex<HashSet<String>>>,
}

struct IssuedSessionTokens {
    response: TokenResponse,
    session_id: String,
    refresh_token_hash: String,
    expires_at_unix: u64,
}

impl PostgresIdentityStore {
    /// Creates a new store backed by `db`, signing JWTs with `secret`.
    /// Creates a new store; tokens expire after 1 hour (3600 s).
    pub fn new(db: Db, secret: impl Into<String>) -> Self {
        Self::new_with_ttl(db, secret, 3600)
    }

    /// Like [`new`](Self::new) but with a configurable TTL in seconds.
    /// Intended for testing (e.g. `JWT_TTL_SECS=30`).
    pub fn new_with_ttl(db: Db, secret: impl Into<String>, token_ttl_secs: u64) -> Self {
        Self {
            db,
            secret: secret.into(),
            _token_ttl_secs: token_ttl_secs,
            revoked_access_tokens: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Creates the `users` and `refresh_sessions` tables if they do not already exist.
    pub async fn setup(&self) -> Result<(), allex_postgres::DbError> {
        self.db
            .with_client(|client| async move {
                client
                    .execute(
                        "CREATE TABLE IF NOT EXISTS users (
                            handle        TEXT        PRIMARY KEY,
                            password_hash TEXT        NOT NULL,
                            role          TEXT        NOT NULL,
                            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
                        )",
                        &[],
                    )
                    .await?;
                client
                    .execute(
                        "CREATE TABLE IF NOT EXISTS refresh_sessions (
                            session_id         TEXT        PRIMARY KEY,
                            handle             TEXT        NOT NULL REFERENCES users(handle) ON DELETE CASCADE,
                            role               TEXT        NOT NULL,
                            refresh_token_hash TEXT        NOT NULL UNIQUE,
                            expires_at         TIMESTAMPTZ NOT NULL,
                            created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                            revoked_at         TIMESTAMPTZ,
                            replaced_by        TEXT
                        )",
                        &[],
                    )
                    .await?;
                client
                    .execute(
                        "CREATE INDEX IF NOT EXISTS refresh_sessions_handle_idx
                         ON refresh_sessions (handle)",
                        &[],
                    )
                    .await?;
                Ok(())
            })
            .await
    }
}

fn db_err(e: impl std::fmt::Display) -> IdentityError {
    IdentityError::Internal(e.to_string())
}

fn hash_refresh_token(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

fn issue_refresh_session_tokens(
    secret: &str,
    handle: &str,
    role: &str,
) -> Result<IssuedSessionTokens, IdentityError> {
    let session_id = Uuid::new_v4().to_string();
    let response = issue_token_response_with_refresh_session(secret, handle, role, &session_id)?;
    let claims = validate_refresh_token(secret, &response.refresh_token)?;

    Ok(IssuedSessionTokens {
        refresh_token_hash: hash_refresh_token(&response.refresh_token),
        expires_at_unix: claims.expires_at_unix,
        response,
        session_id,
    })
}

impl IdentityV1 for PostgresIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let secret = self.secret.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let issued = issue_refresh_session_tokens(&secret, &req.handle, &req.role)?;
            let mut client = db.get_client().await.map_err(db_err)?;
            let tx = client.transaction().await.map_err(db_err)?;

            let rows = tx
                .query(
                    "INSERT INTO users (handle, password_hash, role)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (handle) DO NOTHING
                     RETURNING handle",
                    &[&req.handle, &password_hash, &req.role],
                )
                .await
                .map_err(db_err)?;

            if rows.is_empty() {
                return Err(IdentityError::HandleAlreadyTaken);
            }

            tx.execute(
                "INSERT INTO refresh_sessions (
                    session_id,
                    handle,
                    role,
                    refresh_token_hash,
                    expires_at
                 )
                 VALUES ($1, $2, $3, $4, to_timestamp($5))",
                &[
                    &issued.session_id,
                    &req.handle,
                    &req.role,
                    &issued.refresh_token_hash,
                    &(issued.expires_at_unix as f64),
                ],
            )
            .await
            .map_err(db_err)?;

            tx.commit().await.map_err(db_err)?;
            Ok(issued.response)
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let secret = self.secret.clone();
        async move {
            let client = db.get_client().await.map_err(db_err)?;
            let row = client
                .query_opt(
                    "SELECT password_hash, role FROM users WHERE handle = $1",
                    &[&req.handle],
                )
                .await
                .map_err(db_err)?
                .ok_or(IdentityError::InvalidCredentials)?;

            let password_hash: String = row.try_get("password_hash").map_err(db_err)?;
            let role: String = row.try_get("role").map_err(db_err)?;

            let password = req.password.clone();
            let valid = tokio::task::spawn_blocking(move || {
                verify(password, &password_hash).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            if !valid {
                return Err(IdentityError::InvalidCredentials);
            }

            let issued = issue_refresh_session_tokens(&secret, &req.handle, &role)?;
            client
                .execute(
                    "INSERT INTO refresh_sessions (
                        session_id,
                        handle,
                        role,
                        refresh_token_hash,
                        expires_at
                     )
                     VALUES ($1, $2, $3, $4, to_timestamp($5))",
                    &[
                        &issued.session_id,
                        &req.handle,
                        &role,
                        &issued.refresh_token_hash,
                        &(issued.expires_at_unix as f64),
                    ],
                )
                .await
                .map_err(db_err)?;

            Ok(issued.response)
        }
    }

    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let secret = self.secret.clone();
        async move {
            let claims = validate_refresh_token(&secret, &req.refresh_token)?;
            let session_id = claims
                .refresh_session_id
                .clone()
                .ok_or(IdentityError::InvalidRefreshToken)?;
            let presented_hash = hash_refresh_token(&req.refresh_token);
            let issued = issue_refresh_session_tokens(&secret, &claims.handle, &claims.role)?;

            let mut client = db.get_client().await.map_err(db_err)?;
            let tx = client.transaction().await.map_err(db_err)?;
            let row = tx
                .query_opt(
                    "SELECT handle, role, refresh_token_hash
                     FROM refresh_sessions
                     WHERE session_id = $1
                       AND revoked_at IS NULL
                       AND expires_at > now()
                     FOR UPDATE",
                    &[&session_id],
                )
                .await
                .map_err(db_err)?
                .ok_or(IdentityError::InvalidRefreshToken)?;

            let stored_handle: String = row.try_get("handle").map_err(db_err)?;
            let stored_role: String = row.try_get("role").map_err(db_err)?;
            let stored_hash: String = row.try_get("refresh_token_hash").map_err(db_err)?;

            if stored_handle != claims.handle
                || stored_role != claims.role
                || stored_hash != presented_hash
            {
                return Err(IdentityError::InvalidRefreshToken);
            }

            tx.execute(
                "UPDATE refresh_sessions
                 SET revoked_at = COALESCE(revoked_at, now()),
                     replaced_by = $2
                 WHERE session_id = $1",
                &[&session_id, &issued.session_id],
            )
            .await
            .map_err(db_err)?;

            tx.execute(
                "INSERT INTO refresh_sessions (
                    session_id,
                    handle,
                    role,
                    refresh_token_hash,
                    expires_at
                 )
                 VALUES ($1, $2, $3, $4, to_timestamp($5))",
                &[
                    &issued.session_id,
                    &claims.handle,
                    &claims.role,
                    &issued.refresh_token_hash,
                    &(issued.expires_at_unix as f64),
                ],
            )
            .await
            .map_err(db_err)?;

            tx.commit().await.map_err(db_err)?;
            Ok(issued.response)
        }
    }

    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl Future<Output = Result<RoleResponse, IdentityError>> + Send {
        let secret = self.secret.clone();
        let revoked_access_tokens = self.revoked_access_tokens.clone();
        async move {
            let is_revoked = revoked_access_tokens
                .lock()
                .map_err(|e| IdentityError::Internal(e.to_string()))?
                .contains(&req.token);
            if is_revoked {
                return Err(IdentityError::InvalidToken);
            }

            let claims = validate_access_token(&secret, &req.token)?;
            Ok(RoleResponse { role: claims.role })
        }
    }

    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl Future<Output = Result<LogoutResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let secret = self.secret.clone();
        let revoked_access_tokens = self.revoked_access_tokens.clone();
        async move {
            let mut saw_token = false;

            if let Some(token) = req.token {
                revoked_access_tokens
                    .lock()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?
                    .insert(token);
                saw_token = true;
            }

            if let Some(refresh_token) = req.refresh_token {
                let claims = validate_refresh_token(&secret, &refresh_token)?;
                let session_id = claims
                    .refresh_session_id
                    .ok_or(IdentityError::InvalidRefreshToken)?;
                let updated = db
                    .get_client()
                    .await
                    .map_err(db_err)?
                    .execute(
                        "UPDATE refresh_sessions
                         SET revoked_at = COALESCE(revoked_at, now())
                         WHERE session_id = $1
                           AND refresh_token_hash = $2",
                        &[&session_id, &hash_refresh_token(&refresh_token)],
                    )
                    .await
                    .map_err(db_err)?;
                if updated == 0 {
                    return Err(IdentityError::InvalidRefreshToken);
                }
                saw_token = true;
            }

            if !saw_token {
                return Err(IdentityError::InvalidToken);
            }

            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for PostgresIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        let is_revoked = self
            .revoked_access_tokens
            .lock()
            .map(|set| set.contains(token))
            .unwrap_or(false);
        if is_revoked {
            return None;
        }
        validate_access_token(&self.secret, token)
            .ok()
            .map(|claims| claims.role)
    }
}
