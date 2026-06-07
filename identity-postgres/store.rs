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
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RegisterRequest,
    RoleResponse, TokenResponse, ValidateRequest,
};

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

/// A PostgreSQL-backed identity store.
///
/// Persists users in a `users` table. Call [`PostgresIdentityStore::setup`]
/// once at startup to create the table if it does not already exist.
///
/// Implements both [`IdentityV1`] (register / login / validate / logout) and
/// [`BearerTokenValidator`] (synchronous JWT check used by transports).
#[derive(Clone)]
pub struct PostgresIdentityStore {
    db: Db,
    secret: String,
    /// In-memory blacklist of revoked tokens. Cleared on restart (acceptable since
    /// all JWTs expire after 1 hour). Shared across clones via Arc.
    revoked_tokens: Arc<Mutex<HashSet<String>>>,
}

impl PostgresIdentityStore {
    /// Creates a new store backed by `db`, signing JWTs with `secret`.
    pub fn new(db: Db, secret: impl Into<String>) -> Self {
        Self {
            db,
            secret: secret.into(),
            revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Creates the `users` table if it does not already exist.
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
                Ok(())
            })
            .await
    }

    fn decode_jwt(&self, token: &str) -> Option<Claims> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )
        .ok()
        .map(|d| d.claims)
    }

    fn issue_jwt(&self, sub: &str, role: &str) -> Result<String, IdentityError> {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize
            + 3600;
        let claims = Claims {
            sub: sub.to_string(),
            role: role.to_string(),
            exp,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| IdentityError::Internal(e.to_string()))
    }
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
        let secret = self.secret.clone();
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

            let store = PostgresIdentityStore {
                db,
                secret,
                revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
            };
            let token = store.issue_jwt(&handle, &role)?;
            Ok(TokenResponse {
                token,
                handle,
                role,
            })
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

            let store = PostgresIdentityStore {
                db,
                secret,
                revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
            };
            let token = store.issue_jwt(&req.handle, &role)?;
            Ok(TokenResponse {
                token,
                handle: req.handle,
                role,
            })
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
        let revoked = self.revoked_tokens.clone();
        async move {
            revoked
                .lock()
                .map_err(|e| IdentityError::Internal(e.to_string()))?
                .insert(req.token);
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
