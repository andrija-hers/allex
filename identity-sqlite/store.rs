use std::{future::Future, path::Path, sync::Arc};

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
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use transport::BearerTokenValidator;

const FORGOT_PASSWORD_TEMPLATE_KEY: &str = "forgot_password";

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

/// A SQLite-backed identity store.
#[derive(Clone)]
pub struct SqliteIdentityStore {
    db_path: Arc<std::path::PathBuf>,
    secret: String,
    token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
}

impl SqliteIdentityStore {
    pub fn new(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
    ) -> Result<Self, std::io::Error> {
        Self::new_with_ttl(path, secret, 3600, 7 * 24 * 3600)
    }

    pub fn new_with_ttl(
        path: impl AsRef<Path>,
        secret: impl Into<String>,
        token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
    ) -> Result<Self, std::io::Error> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self {
            db_path: Arc::new(path),
            secret: secret.into(),
            token_ttl_secs,
            refresh_token_ttl_secs,
        })
    }

    fn open_conn(&self) -> Result<Connection, IdentityError> {
        Connection::open(self.db_path.as_path()).map_err(db_err)
    }

    pub async fn setup(&self) -> Result<(), IdentityError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
            let conn = store.open_conn()?;
            conn.execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS users (
                    handle TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 CREATE TABLE IF NOT EXISTS refresh_tokens (
                    token TEXT PRIMARY KEY,
                    handle TEXT NOT NULL,
                    role TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    FOREIGN KEY (handle) REFERENCES users(handle) ON DELETE CASCADE
                 );
                 CREATE TABLE IF NOT EXISTS revoked_tokens (
                    token TEXT PRIMARY KEY,
                    expires_at INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS message_templates (
                    template_key TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    body TEXT NOT NULL,
                    PRIMARY KEY (template_key, channel)
                 );",
            )
            .map_err(db_err)?;
            conn.execute(
                "DELETE FROM refresh_tokens WHERE expires_at <= ?1",
                params![now_secs() as u64],
            )
            .map_err(db_err)?;
            conn.execute(
                "DELETE FROM revoked_tokens WHERE expires_at <= ?1",
                params![now_secs() as u64],
            )
            .map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| IdentityError::Internal(e.to_string()))?
    }

    pub async fn upsert_message_template(
        &self,
        req: UpsertMessageTemplateRequest,
    ) -> Result<(), TemplateError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), TemplateError> {
            let conn = Connection::open(store.db_path.as_path()).map_err(template_db_err)?;
            conn.execute(
                "INSERT INTO message_templates (template_key, channel, body)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(template_key, channel) DO UPDATE SET body = excluded.body",
                params![req.template_key, req.channel.as_str(), req.body],
            )
            .map_err(template_db_err)?;
            Ok(())
        })
        .await
        .map_err(|e| TemplateError::Storage(e.to_string()))?
    }

    pub async fn render_message_template(
        &self,
        req: RenderMessageTemplateRequest,
    ) -> Result<RenderedMessageResponse, TemplateError> {
        let store = self.clone();
        let body = tokio::task::spawn_blocking(move || -> Result<String, TemplateError> {
            let conn = Connection::open(store.db_path.as_path()).map_err(template_db_err)?;
            conn.query_row(
                "SELECT body FROM message_templates WHERE template_key = ?1 AND channel = ?2",
                params![req.template_key, req.channel.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(template_db_err)?
            .ok_or(TemplateError::NotFound)
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
                template_key: FORGOT_PASSWORD_TEMPLATE_KEY.into(),
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

    async fn issue_refresh_token(&self, handle: &str, role: &str) -> Result<String, IdentityError> {
        let store = self.clone();
        let handle = handle.to_owned();
        let role = role.to_owned();
        tokio::task::spawn_blocking(move || -> Result<String, IdentityError> {
            let conn = store.open_conn()?;
            let token = uuid::Uuid::new_v4().to_string();
            let expires_at = now_secs() as u64 + store.refresh_token_ttl_secs;
            conn.execute(
                "INSERT INTO refresh_tokens (token, handle, role, expires_at) VALUES (?1, ?2, ?3, ?4)",
                params![token, handle, role, expires_at],
            )
            .map_err(db_err)?;
            Ok(token)
        })
        .await
        .map_err(|e| IdentityError::Internal(e.to_string()))?
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

fn template_db_err(e: impl std::fmt::Display) -> TemplateError {
    TemplateError::Storage(e.to_string())
}

fn revocation_expiry(token: &str, fallback_ttl_secs: u64, secret: &str) -> u64 {
    let mut validation = Validation::default();
    validation.leeway = 0;
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()
    .map(|decoded| decoded.claims.exp as u64)
    .filter(|expiry| *expiry > now_secs() as u64)
    .unwrap_or_else(|| now_secs() as u64 + fallback_ttl_secs)
}

impl IdentityV1 for SqliteIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let store = self.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let s = store.clone();
            let handle = req.handle.clone();
            let role = req.role.clone();
            tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
                let conn = s.open_conn()?;
                let affected = conn
                    .execute(
                        "INSERT OR IGNORE INTO users (handle, password_hash, role) VALUES (?1, ?2, ?3)",
                        params![handle, password_hash, role],
                    )
                    .map_err(db_err)?;
                if affected == 0 {
                    return Err(IdentityError::HandleAlreadyTaken);
                }
                Ok(())
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let token = store.issue_jwt(&req.handle, &req.role)?;
            let refresh_token = store.issue_refresh_token(&req.handle, &req.role).await?;
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
        let store = self.clone();
        async move {
            let s = store.clone();
            let handle = req.handle.clone();
            let (password_hash, role) = tokio::task::spawn_blocking(move || -> Result<(String, String), IdentityError> {
                let conn = s.open_conn()?;
                conn.query_row(
                    "SELECT password_hash, role FROM users WHERE handle = ?1",
                    params![handle],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(db_err)?
                .ok_or(IdentityError::InvalidCredentials)
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

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
        let store = self.clone();
        async move {
            let s = store.clone();
            let rt = req.refresh_token.clone();
            let (handle, role) = tokio::task::spawn_blocking(move || -> Result<(String, String), IdentityError> {
                let mut conn = s.open_conn()?;
                let tx = conn.transaction().map_err(db_err)?;

                let row = tx
                    .query_row(
                        "SELECT handle, role FROM refresh_tokens WHERE token = ?1 AND expires_at > ?2",
                        params![rt, now_secs() as u64],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()
                    .map_err(db_err)?
                    .ok_or(IdentityError::InvalidRefreshToken)?;

                tx.execute("DELETE FROM refresh_tokens WHERE token = ?1", params![req.refresh_token])
                    .map_err(db_err)?;
                tx.execute(
                    "DELETE FROM refresh_tokens WHERE handle = ?1 AND expires_at <= ?2",
                    params![row.0.clone(), now_secs() as u64],
                )
                .map_err(db_err)?;
                tx.commit().map_err(db_err)?;
                Ok(row)
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let token = store.issue_jwt(&handle, &role)?;
            let new_refresh = store.issue_refresh_token(&handle, &role).await?;
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
        let store = self.clone();
        async move {
            let token = req.token;
            let refresh_token = req.refresh_token;
            let s = store.clone();
            tokio::task::spawn_blocking(move || -> Result<(), IdentityError> {
                let conn = s.open_conn()?;

                if let Some(token) = token {
                    let expires_at = revocation_expiry(&token, s.token_ttl_secs, &s.secret);
                    conn.execute(
                        "INSERT OR REPLACE INTO revoked_tokens (token, expires_at) VALUES (?1, ?2)",
                        params![token, expires_at],
                    )
                    .map_err(db_err)?;
                }

                if let Some(rt) = refresh_token {
                    conn.execute("DELETE FROM refresh_tokens WHERE token = ?1", params![rt])
                        .map_err(db_err)?;
                }

                conn.execute(
                    "DELETE FROM revoked_tokens WHERE expires_at <= ?1",
                    params![now_secs() as u64],
                )
                .map_err(db_err)?;
                Ok(())
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;
            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for SqliteIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        let is_revoked = self
            .open_conn()
            .and_then(|conn| {
                conn.query_row(
                    "SELECT 1 FROM revoked_tokens WHERE token = ?1 AND expires_at > ?2",
                    params![token, now_secs() as u64],
                    |_| Ok(()),
                )
                .optional()
                .map(|row| row.is_some())
                .map_err(db_err)
            })
            .unwrap_or(true);
        if is_revoked {
            return None;
        }
        self.decode_jwt(token)
            .map(|c| format!("{}\x1e{}", c.sub, c.role))
    }
}
