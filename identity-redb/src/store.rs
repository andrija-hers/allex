use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bcrypt::{DEFAULT_COST, hash, verify};
use identity::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RefreshRequest,
    RegisterRequest, RoleResponse, TokenResponse, ValidateRequest,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use message_templates::{
    ForgotPasswordMessageRequest, ForgotPasswordMessageResponse, RenderMessageTemplateRequest,
    RenderedMessageResponse, TemplateError, UpsertMessageTemplateRequest, render_stored_template,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use transport::BearerTokenValidator;

const USERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const REFRESH_TOKENS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("refresh_tokens");
const MESSAGE_TEMPLATES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("message_templates");

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRefreshToken {
    handle: String,
    role: String,
    expires_at: u64,
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
#[derive(Clone)]
pub struct RedbIdentityStore {
    database: Arc<Database>,
    secret: String,
    token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
    revoked_tokens: Arc<Mutex<HashSet<String>>>,
}

impl RedbIdentityStore {
    pub fn new(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
    ) -> Result<Self, RedbIdentityStoreError> {
        Self::new_with_ttl(path, secret, 3600, 7 * 24 * 3600)
    }

    pub fn new_with_ttl(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
        token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
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
            token_ttl_secs,
            refresh_token_ttl_secs,
            revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub async fn setup(&self) -> Result<(), RedbIdentityStoreError> {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let write_tx = database.begin_write().map_err(redb_err)?;
            {
                let _ = write_tx.open_table(USERS_TABLE).map_err(redb_err)?;
                let _ = write_tx
                    .open_table(REFRESH_TOKENS_TABLE)
                    .map_err(redb_err)?;
            }
            write_tx.commit().map_err(redb_err)?;
            Ok(())
        })
        .await
        .map_err(join_err)?
    }

    pub async fn upsert_message_template(
        &self,
        req: UpsertMessageTemplateRequest,
    ) -> Result<(), TemplateError> {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || -> Result<(), TemplateError> {
            let write_tx = database
                .begin_write()
                .map_err(template_storage_err)?;
            {
                let mut table = write_tx
                    .open_table(MESSAGE_TEMPLATES_TABLE)
                    .map_err(template_storage_err)?;
                table
                    .insert(
                        format!("{}:{}", req.template_key, req.channel.as_str()).as_str(),
                        req.body.as_bytes(),
                    )
                    .map_err(template_storage_err)?;
            }
            write_tx.commit().map_err(template_storage_err)?;
            Ok(())
        })
        .await
        .map_err(|e| TemplateError::Storage(e.to_string()))?
    }

    pub async fn render_message_template(
        &self,
        req: RenderMessageTemplateRequest,
    ) -> Result<RenderedMessageResponse, TemplateError> {
        let database = self.database.clone();
        let key = format!("{}:{}", req.template_key, req.channel.as_str());
        let body = tokio::task::spawn_blocking(move || -> Result<String, TemplateError> {
            let read_tx = database
                .begin_read()
                .map_err(template_storage_err)?;
            let table = read_tx
                .open_table(MESSAGE_TEMPLATES_TABLE)
                .map_err(template_storage_err)?;
            let value = table
                .get(key.as_str())
                .map_err(template_storage_err)?
                .ok_or(TemplateError::NotFound)?;
            String::from_utf8(value.value().to_vec())
                .map_err(template_storage_err)
        })
        .await
        .map_err(|e| TemplateError::Storage(e.to_string()))??;
        let message = render_stored_template(&body, &req.data)?;
        Ok(RenderedMessageResponse { message })
    }

    pub async fn build_forgot_password_message(
        &self,
        req: ForgotPasswordMessageRequest,
    ) -> Result<ForgotPasswordMessageResponse, TemplateError> {
        let rendered = self
            .render_message_template(RenderMessageTemplateRequest {
                template_key: "forgot_password".into(),
                channel: req.channel,
                data: serde_json::json!({
                    "handle": req.handle,
                    "reset_url": req.reset_url,
                }),
            })
            .await?;
        Ok(ForgotPasswordMessageResponse {
            message: rendered.message,
        })
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
        let exp = now_secs() + self.token_ttl_secs as usize;
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

    /// Synchronous helper: write a new refresh token to redb, returns the token string.
    fn store_refresh_token_sync(
        database: &Database,
        handle: &str,
        role: &str,
        ttl_secs: u64,
    ) -> Result<String, IdentityError> {
        let token = uuid::Uuid::new_v4().to_string();
        let stored = StoredRefreshToken {
            handle: handle.to_owned(),
            role: role.to_owned(),
            expires_at: now_secs() as u64 + ttl_secs,
        };
        let encoded =
            serde_json::to_vec(&stored).map_err(|e| IdentityError::Internal(e.to_string()))?;
        let write_tx = database
            .begin_write()
            .map_err(|e| IdentityError::Internal(e.to_string()))?;
        {
            let mut table = write_tx
                .open_table(REFRESH_TOKENS_TABLE)
                .map_err(|e| IdentityError::Internal(e.to_string()))?;
            table
                .insert(token.as_str(), encoded.as_slice())
                .map_err(|e| IdentityError::Internal(e.to_string()))?;
        }
        write_tx
            .commit()
            .map_err(|e| IdentityError::Internal(e.to_string()))?;
        Ok(token)
    }
}

fn now_secs() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
}

impl IdentityV1 for RedbIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.database.clone();
        let secret = self.secret.clone();
        let token_ttl = self.token_ttl_secs;
        let refresh_ttl = self.refresh_token_ttl_secs;
        let revoked = self.revoked_tokens.clone();
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
            let db_write = db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
                let write_tx = db_write
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

            // Issue JWT
            let store = RedbIdentityStore {
                database: db.clone(),
                secret,
                token_ttl_secs: token_ttl,
                refresh_token_ttl_secs: refresh_ttl,
                revoked_tokens: revoked,
            };
            let token = store.issue_jwt(&req.handle, &req.role)?;

            // Issue refresh token
            let h = req.handle.clone();
            let r = req.role.clone();
            let refresh_token = tokio::task::spawn_blocking(move || {
                RedbIdentityStore::store_refresh_token_sync(&db, &h, &r, refresh_ttl)
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            Ok(TokenResponse {
                token,
                refresh_token: Some(refresh_token),
                handle: req.handle,
                role: req.role,
            })
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let db = self.database.clone();
        let secret = self.secret.clone();
        let token_ttl = self.token_ttl_secs;
        let refresh_ttl = self.refresh_token_ttl_secs;
        let revoked = self.revoked_tokens.clone();
        async move {
            let handle = req.handle.clone();
            let db_read = db.clone();
            let stored =
                tokio::task::spawn_blocking(move || -> Result<StoredUser, IdentityError> {
                    let read_tx = db_read
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
                database: db.clone(),
                secret,
                token_ttl_secs: token_ttl,
                refresh_token_ttl_secs: refresh_ttl,
                revoked_tokens: revoked,
            };
            let token = store.issue_jwt(&req.handle, &role)?;

            let h = req.handle.clone();
            let r = role.clone();
            let refresh_token = tokio::task::spawn_blocking(move || {
                RedbIdentityStore::store_refresh_token_sync(&db, &h, &r, refresh_ttl)
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

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
        let db = self.database.clone();
        let db_for_jwt = self.database.clone();
        let secret = self.secret.clone();
        let token_ttl = self.token_ttl_secs;
        let refresh_ttl = self.refresh_token_ttl_secs;
        let revoked = self.revoked_tokens.clone();
        async move {
            let rt_key = req.refresh_token.clone();

            // Atomically consume old refresh token and issue a new one in one blocking task
            let (handle, role, new_refresh) = tokio::task::spawn_blocking(
                move || -> Result<(String, String, String), IdentityError> {
                    // Delete old token
                    let write_tx = db
                        .begin_write()
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    let (handle, role) = {
                        let mut table = write_tx
                            .open_table(REFRESH_TOKENS_TABLE)
                            .map_err(|e| IdentityError::Internal(e.to_string()))?;
                        let entry = table
                            .remove(rt_key.as_str())
                            .map_err(|e| IdentityError::Internal(e.to_string()))?
                            .ok_or(IdentityError::InvalidRefreshToken)?;
                        let stored: StoredRefreshToken = serde_json::from_slice(entry.value())
                            .map_err(|e| IdentityError::Internal(e.to_string()))?;
                        if stored.expires_at <= now_secs() as u64 {
                            return Err(IdentityError::InvalidRefreshToken);
                        }
                        (stored.handle, stored.role)
                    };
                    write_tx
                        .commit()
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;

                    // Issue new refresh token
                    let new_refresh = RedbIdentityStore::store_refresh_token_sync(
                        &db,
                        &handle,
                        &role,
                        refresh_ttl,
                    )?;
                    Ok((handle, role, new_refresh))
                },
            )
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let store = RedbIdentityStore {
                database: db_for_jwt,
                secret,
                token_ttl_secs: token_ttl,
                refresh_token_ttl_secs: refresh_ttl,
                revoked_tokens: revoked,
            };
            let token = store.issue_jwt(&handle, &role)?;

            Ok(TokenResponse {
                token,
                refresh_token: Some(new_refresh),
                handle,
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
        let database = self.database.clone();
        async move {
            if let Some(token) = req.token {
                revoked
                    .lock()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?
                    .insert(token);
            }
            if let Some(rt) = req.refresh_token {
                tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
                    let write_tx = database
                        .begin_write()
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    {
                        let mut table = write_tx
                            .open_table(REFRESH_TOKENS_TABLE)
                            .map_err(|e| IdentityError::Internal(e.to_string()))?;
                        let _ = table
                            .remove(rt.as_str())
                            .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    }
                    write_tx
                        .commit()
                        .map_err(|e| IdentityError::Internal(e.to_string()))?;
                    Ok(())
                })
                .await
                .map_err(|e| IdentityError::Internal(e.to_string()))??;
            }
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
        self.decode_jwt(token)
            .map(|c| format!("{}\x1e{}", c.sub, c.role))
    }
}

fn redb_err(e: impl std::fmt::Display) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(e.to_string())
}

fn join_err(e: tokio::task::JoinError) -> RedbIdentityStoreError {
    RedbIdentityStoreError::Redb(format!("task join error: {e}"))
}

fn template_storage_err(e: impl std::fmt::Display) -> TemplateError {
    TemplateError::Storage(e.to_string())
}
