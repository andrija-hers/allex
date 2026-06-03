use std::future::Future;

use allex_postgres::Db;
use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use transport::BearerTokenValidator;

use identity::{
    IdentityError, IdentityV1, LoginRequest, RegisterRequest, RoleResponse, TokenResponse,
    ValidateRequest,
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
/// Implements both [`IdentityV1`] (register / login / validate) and
/// [`BearerTokenValidator`] (synchronous JWT check used by transports).
#[derive(Clone)]
pub struct PostgresIdentityStore {
    db: Db,
    secret: String,
}

impl PostgresIdentityStore {
    /// Creates a new store backed by `db`, signing JWTs with `secret`.
    pub fn new(db: Db, secret: impl Into<String>) -> Self {
        Self {
            db,
            secret: secret.into(),
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

            // INSERT … ON CONFLICT DO NOTHING lets us check handle uniqueness
            // and insert in one roundtrip; an empty RETURNING means it was taken.
            let rows = client
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

            let store = PostgresIdentityStore { db, secret };
            let token = store.issue_jwt(&req.handle, &req.role)?;
            Ok(TokenResponse { token })
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

            let store = PostgresIdentityStore { db, secret };
            let token = store.issue_jwt(&req.handle, &role)?;
            Ok(TokenResponse { token })
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
}

impl BearerTokenValidator for PostgresIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        self.decode_jwt(token).map(|c| c.role)
    }
}
