use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bcrypt::{DEFAULT_COST, hash, verify};
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RefreshRequest,
    RegisterRequest, RoleResponse, TokenResponse, ValidateRequest, issue_token_response,
    validate_access_token, validate_refresh_token,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use transport::BearerTokenValidator;

const USERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("users");

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
    revoked_access_tokens: Arc<Mutex<HashSet<String>>>,
    revoked_refresh_tokens: Arc<Mutex<HashSet<String>>>,
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
            revoked_access_tokens: Arc::new(Mutex::new(HashSet::new())),
            revoked_refresh_tokens: Arc::new(Mutex::new(HashSet::new())),
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
}

impl IdentityV1 for RedbIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let database = self.database.clone();
        let secret = self.secret.clone();
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

            drop(database);
            issue_token_response(&secret, &req.handle, &req.role)
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let database = self.database.clone();
        let secret = self.secret.clone();
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
            drop(database);
            issue_token_response(&secret, &req.handle, &role)
        }
    }

    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let secret = self.secret.clone();
        let revoked_refresh_tokens = self.revoked_refresh_tokens.clone();
        async move {
            let claims = validate_refresh_token(&secret, &req.refresh_token)?;
            let mut revoked = revoked_refresh_tokens
                .lock()
                .map_err(|e| IdentityError::Internal(e.to_string()))?;
            if revoked.contains(&req.refresh_token) {
                return Err(IdentityError::InvalidRefreshToken);
            }
            revoked.insert(req.refresh_token);
            drop(revoked);

            issue_token_response(&secret, &claims.handle, &claims.role)
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
        let revoked_access_tokens = self.revoked_access_tokens.clone();
        let revoked_refresh_tokens = self.revoked_refresh_tokens.clone();
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
                revoked_refresh_tokens
                    .lock()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?
                    .insert(refresh_token);
                saw_token = true;
            }

            if !saw_token {
                return Err(IdentityError::InvalidToken);
            }

            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for RedbIdentityStore {
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

fn redb_err(e: impl std::fmt::Display) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(e.to_string())
}

fn join_err(e: tokio::task::JoinError) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(format!("task join error: {e}"))
}
