use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
};

use allex_postgres::Db;
use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use transport::BearerTokenValidator;

use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RefreshRequest,
    RegisterRequest, RoleResponse, TokenResponse, ValidateRequest,
};

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

/// A PostgreSQL-backed identity store.
///
/// Persists users in a `users` table and refresh tokens in a `refresh_tokens`
/// table. Call [`PostgresIdentityStore::setup`] once at startup.
#[derive(Clone)]
pub struct PostgresIdentityStore {
    db: Db,
    secret: String,
    token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
    /// In-memory blacklist of revoked access tokens. Cleared on restart.
    revoked_tokens: Arc<Mutex<HashSet<String>>>,
}

impl PostgresIdentityStore {
    /// Creates a new store; access tokens expire after 1 hour, refresh tokens after 7 days.
    pub fn new(db: Db, secret: impl Into<String>) -> Self {
        Self::new_with_ttl(db, secret, 3600, 7 * 24 * 3600)
    }

    /// Like [`new`](Self::new) but with configurable TTLs in seconds.
    pub fn new_with_ttl(
        db: Db,
        secret: impl Into<String>,
        token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
    ) -> Self {
        Self {
            db,
            secret: secret.into(),
            token_ttl_secs,
            refresh_token_ttl_secs,
            revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Creates the `users` and `refresh_tokens` tables if they do not already exist.
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
                        "CREATE TABLE IF NOT EXISTS refresh_tokens (
                            token      TEXT        PRIMARY KEY,
                            handle     TEXT        NOT NULL REFERENCES users(handle) ON DELETE CASCADE,
                            role       TEXT        NOT NULL,
                            expires_at TIMESTAMPTZ NOT NULL
                        )",
                        &[],
                    )
                    .await?;
                Ok(())
            })
            .await
    }

    fn decode_jwt(&self, token: &str) -> Option<Claims> {
        let mut validation = Validation::default();
        validation.leeway = 0;
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .ok()
        .map(|d| d.claims)
    }

    fn issue_jwt(&self, sub: &str, role: &str) -> Result<String, IdentityError> {
        let exp = now_secs() + self.token_ttl_secs as usize;
        let claims = Claims { sub: sub.to_string(), role: role.to_string(), exp };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| IdentityError::Internal(e.to_string()))
    }

    async fn issue_refresh_token(&self, handle: &str, role: &str) -> Result<String, IdentityError> {
        let token = uuid::Uuid::new_v4().to_string();
        let ttl = self.refresh_token_ttl_secs.to_string();
        let db = self.db.clone();
        let handle = handle.to_string();
        let role = role.to_string();
        let token_clone = token.clone();
        db.get_client()
            .await
            .map_err(db_err)?
            .execute(
                "INSERT INTO refresh_tokens (token, handle, role, expires_at)
                 VALUES ($1, $2, $3, now() + ($4 || ' seconds')::interval)",
                &[&token_clone, &handle, &role, &ttl],
            )
            .await
            .map_err(db_err)?;
        Ok(token)
    }
}

fn now_secs() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
}

fn db_err(e: impl std::fmt::Display) -> IdentityError {
    IdentityError::Internal(e.to_string())
}

impl IdentityV1 for PostgresIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let store = self.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let client = db.get_client().await.map_err(db_err)?;

            let rows = client
                .query(
                    "INSERT INTO users (handle, password_hash, role)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (handle) DO NOTHING
                     RETURNING handle, role",
                    &[&req.handle, &password_hash, &req.role],
                )
                .await
                .map_err(db_err)?;

            if rows.is_empty() {
                return Err(IdentityError::HandleAlreadyTaken);
            }

            let handle: String = rows[0].try_get("handle").map_err(db_err)?;
            let role: String = rows[0].try_get("role").map_err(db_err)?;

            let token = store.issue_jwt(&handle, &role)?;
            let refresh_token = store.issue_refresh_token(&handle, &role).await?;
            Ok(TokenResponse { token, refresh_token: Some(refresh_token), handle, role })
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let store = self.clone();
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

            let token = store.issue_jwt(&req.handle, &role)?;
            let refresh_token = store.issue_refresh_token(&req.handle, &role).await?;
            Ok(TokenResponse {
                token,
                refresh_token: Some(refresh_token),
                handle: req.handle,
                role,
            })
        }
    }

    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let store = self.clone();
        async move {
            let client = db.get_client().await.map_err(db_err)?;

            // Atomically consume the refresh token (DELETE + return data)
            let row = client
                .query_opt(
                    "DELETE FROM refresh_tokens
                     WHERE token = $1 AND expires_at > now()
                     RETURNING handle, role",
                    &[&req.refresh_token],
                )
                .await
                .map_err(db_err)?
                .ok_or(IdentityError::InvalidRefreshToken)?;

            let handle: String = row.try_get("handle").map_err(db_err)?;
            let role: String = row.try_get("role").map_err(db_err)?;

            // Opportunistically clean up any remaining expired tokens for this user
            let _ = client
                .execute(
                    "DELETE FROM refresh_tokens WHERE handle = $1 AND expires_at <= now()",
                    &[&handle],
                )
                .await;

            let token = store.issue_jwt(&handle, &role)?;
            let new_refresh = store.issue_refresh_token(&handle, &role).await?;
            Ok(TokenResponse { token, refresh_token: Some(new_refresh), handle, role })
        }
    }

    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl Future<Output = Result<RoleResponse, IdentityError>> + Send {
        let secret = self.secret.clone();
        async move {
            let claims = decode::<Claims>(
                &req.token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &Validation::default(),
            )
            .map_err(|_| IdentityError::InvalidToken)?
            .claims;
            Ok(RoleResponse { role: claims.role })
        }
    }

    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl Future<Output = Result<LogoutResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let revoked = self.revoked_tokens.clone();
        async move {
            if let Some(token) = req.token {
                revoked
                    .lock()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?
                    .insert(token);
            }
            if let Some(rt) = req.refresh_token {
                let client = db.get_client().await.map_err(db_err)?;
                let _ = client
                    .execute("DELETE FROM refresh_tokens WHERE token = $1", &[&rt])
                    .await;
            }
            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for PostgresIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        let is_revoked = self
            .revoked_tokens
            .lock()
            .map(|set| set.contains(token))
            .unwrap_or(false);
        if is_revoked {
            return None;
        }
        self.decode_jwt(token).map(|c| c.role)
    }
}