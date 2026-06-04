use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bcrypt::{DEFAULT_COST, hash, verify};
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RegisterRequest,
    RoleResponse, TokenResponse, ValidateRequest,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use transport::BearerTokenValidator;

const USERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("users");

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredUser {
    password_hash: String,
    role: String,
}

#[derive(Debug, Error)]
pub enum RedbIdentityStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("redb error: {0}")]
    Redb(String),
}

/// A redb-backed identity store.
///
/// Persists users in a `users` table inside the provided database file. Call
/// [`RedbIdentityStore::setup`] once at startup to create the table if it does
/// not already exist.
#[derive(Clone)]
pub struct RedbIdentityStore {
    database: Arc<Database>,
    secret: String,
    revoked_tokens: Arc<Mutex<HashSet<String>>>,
}

impl RedbIdentityStore {
    /// Opens or creates a redb database at `path`, signing JWTs with `secret`.
    pub fn new(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
    ) -> Result<Self, RedbIdentityStoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let database = if path.exists() {
            Database::open(path).map_err(redb_err)?
        } else {
            Database::create(path).map_err(redb_err)?
        };

        Ok(Self {
            database: Arc::new(database),
            secret: secret.into(),
            revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Creates the `users` table if it does not already exist.
    pub async fn setup(&self) -> Result<(), RedbIdentityStoreError> {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let write_tx = database.begin_write().map_err(redb_err)?;
            {
                let _ = write_tx.open_table(USERS_TABLE).map_err(redb_err)?;
            }
            write_tx.commit().map_err(redb_err)?;
            Ok(())
        })
        .await
        .map_err(join_err)?
    }

    fn decode_jwt(&self, token: &str) -> Option<Claims> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )
        .ok()
        .map(|decoded| decoded.claims)
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

impl IdentityV1 for RedbIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let database = self.database.clone();
        let secret = self.secret.clone();
        let revoked_tokens = self.revoked_tokens.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let handle = req.handle.clone();
            let role = req.role.clone();
            let hash_for_store = password_hash.clone();
            let database_for_write = database.clone();
            tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
                let write_tx = database_for_write
                    .begin_write()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?;
                {
                    let mut table = write_tx
                        .open_table(USERS_TABLE)
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    if table
                        .get(handle.as_str())
                        .map_err(|e| IdentityError::Internal(e.to_string()))?
                        .is_some()
                    {
                        return Err(IdentityError::HandleAlreadyTaken);
                    }
                    let stored = StoredUser {
                        password_hash: hash_for_store,
                        role,
                    };
                    let encoded = serde_json::to_vec(&stored)
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    table
                        .insert(handle.as_str(), encoded.as_slice())
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                }
                write_tx
                    .commit()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?;
                Ok(())
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let store = RedbIdentityStore {
                database,
                secret,
                revoked_tokens,
            };
            let token = store.issue_jwt(&req.handle, &req.role)?;
            Ok(TokenResponse {
                token,
                handle: req.handle,
                role: req.role,
            })
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let database = self.database.clone();
        let secret = self.secret.clone();
        let revoked_tokens = self.revoked_tokens.clone();
        async move {
            let handle = req.handle.clone();
            let database_for_read = database.clone();
            let stored =
                tokio::task::spawn_blocking(move || -> Result<StoredUser, IdentityError> {
                    let read_tx = database_for_read
                        .begin_read()
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    let table = read_tx
                        .open_table(USERS_TABLE)
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    let Some(value) = table
                        .get(handle.as_str())
                        .map_err(|e| IdentityError::Internal(e.to_string()))?
                    else {
                        return Err(IdentityError::InvalidCredentials);
                    };
                    serde_json::from_slice(value.value())
                        .map_err(|e| IdentityError::Internal(e.to_string()))
                })
                .await
                .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let password = req.password.clone();
            let password_hash = stored.password_hash.clone();
            let valid = tokio::task::spawn_blocking(move || {
                verify(password, &password_hash).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            if !valid {
                return Err(IdentityError::InvalidCredentials);
            }

            let role = stored.role;
            let store = RedbIdentityStore {
                database,
                secret,
                revoked_tokens,
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

impl BearerTokenValidator for RedbIdentityStore {
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

fn redb_err(e: impl std::fmt::Display) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(e.to_string())
}

fn join_err(e: tokio::task::JoinError) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(format!("task join error: {e}"))
}
