use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
};

use allex_postgres::Db;
use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use message_templates::{
    ForgotPasswordMessageRequest, ForgotPasswordMessageResponse, RenderMessageTemplateRequest,
    RenderedMessageResponse, TemplateError, UpsertMessageTemplateRequest, render_stored_template,
};
use serde::{Deserialize, Serialize};
use transport::BearerTokenValidator;

use identity::{
    AccountRecord, AccountStore, IdentityError, IdentityV1, LoginRequest, LogoutRequest,
    LogoutResponse, RefreshRequest, RegisterRequest, RevokeRefreshTokenRequest,
    RevokeRefreshTokenResponse, RoleResponse, TokenResponse, ValidateRequest,
};

const FORGOT_PASSWORD_TEMPLATE_KEY: &str = "forgot_password";

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
                            expires_at TIMESTAMPTZ NOT NULL,
                            -- Rotation grace: when a token is consumed it is marked
                            -- `rotated_at` and points at its `replaced_by` successor,
                            -- so a near-simultaneous second use (multi-tab / retry)
                            -- gets the successor instead of being logged out.
                            rotated_at  TIMESTAMPTZ,
                            replaced_by TEXT
                        )",
                        &[],
                    )
                    .await?;
                // Migrate pre-existing deployments (additive, safe).
                client
                    .execute(
                        "ALTER TABLE refresh_tokens \
                         ADD COLUMN IF NOT EXISTS rotated_at TIMESTAMPTZ, \
                         ADD COLUMN IF NOT EXISTS replaced_by TEXT",
                        &[],
                    )
                    .await?;
                // Index the handle: refresh() runs a per-call cleanup DELETE
                // scoped by handle, and logout/password-change delete by handle.
                client
                    .execute(
                        "CREATE INDEX IF NOT EXISTS idx_refresh_tokens_handle \
                         ON refresh_tokens (handle)",
                        &[],
                    )
                    .await?;
                client
                    .execute(
                        "CREATE TABLE IF NOT EXISTS message_templates (
                            template_key TEXT NOT NULL,
                            channel      TEXT NOT NULL,
                            body         TEXT NOT NULL,
                            PRIMARY KEY (template_key, channel)
                        )",
                        &[],
                    )
                    .await?;
                Ok(())
            })
            .await
    }

    pub async fn upsert_message_template(
        &self,
        req: UpsertMessageTemplateRequest,
    ) -> Result<(), TemplateError> {
        let client = self
            .db
            .get_client()
            .await
            .map_err(|e| TemplateError::Storage(e.to_string()))?;
        client
            .execute(
                "INSERT INTO message_templates (template_key, channel, body)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (template_key, channel)
                 DO UPDATE SET body = EXCLUDED.body",
                &[&req.template_key, &req.channel.as_str(), &req.body],
            )
            .await
            .map_err(|e| TemplateError::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn render_message_template(
        &self,
        req: RenderMessageTemplateRequest,
    ) -> Result<RenderedMessageResponse, TemplateError> {
        let client = self
            .db
            .get_client()
            .await
            .map_err(|e| TemplateError::Storage(e.to_string()))?;
        let row = client
            .query_opt(
                "SELECT body FROM message_templates WHERE template_key = $1 AND channel = $2",
                &[&req.template_key, &req.channel.as_str()],
            )
            .await
            .map_err(|e| TemplateError::Storage(e.to_string()))?
            .ok_or(TemplateError::NotFound)?;
        let body: String = row
            .try_get("body")
            .map_err(|e| TemplateError::Storage(e.to_string()))?;
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

            // Release this connection before issue_refresh_token acquires its own,
            // or concurrent registrations deadlock the pool (see login).
            drop(rows);
            drop(client);

            let token = store.issue_jwt(&handle, &role)?;
            let refresh_token = store.issue_refresh_token(&handle, &role).await?;
            Ok(TokenResponse {
                token,
                refresh_token: Some(refresh_token),
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

            // Release the pooled connection before bcrypt + issue_refresh_token
            // (which acquires its own). Holding this one across the second
            // acquisition deadlocks the pool under concurrent logins.
            drop(row);
            drop(client);

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

            // Single-use rotation WITH a grace window, performed atomically in a
            // single statement so a racing second use can never observe a
            // half-rotated token (rotated but not yet linked to its successor):
            // claim the token (only if not yet rotated and unexpired), point it
            // at a freshly-minted successor, and insert that successor — all of
            // which become visible together the instant this statement commits.
            let new_refresh = uuid::Uuid::new_v4().to_string();
            let ttl = store.refresh_token_ttl_secs.to_string();
            let claimed = client
                .query_opt(
                    "WITH claimed AS (
                         UPDATE refresh_tokens
                            SET rotated_at = now(), replaced_by = $2
                          WHERE token = $1 AND rotated_at IS NULL AND expires_at > now()
                      RETURNING handle, role
                     )
                     INSERT INTO refresh_tokens (token, handle, role, expires_at)
                     SELECT $2, handle, role, now() + ($3 || ' seconds')::interval
                       FROM claimed
                     RETURNING handle, role",
                    &[&req.refresh_token, &new_refresh, &ttl],
                )
                .await
                .map_err(db_err)?;

            if let Some(row) = claimed {
                // First (winning) use: a fresh successor was just inserted and the
                // old token now points at it.
                let handle: String = row.try_get("handle").map_err(db_err)?;
                let role: String = row.try_get("role").map_err(db_err)?;
                let token = store.issue_jwt(&handle, &role)?;
                // Tidy up: drop expired tokens and fully-rotated ones past grace.
                let _ = client
                    .execute(
                        "DELETE FROM refresh_tokens
                         WHERE handle = $1
                           AND (expires_at <= now()
                                OR rotated_at < now() - interval '1 hour')",
                        &[&handle],
                    )
                    .await;
                return Ok(TokenResponse {
                    token,
                    refresh_token: Some(new_refresh),
                    handle,
                    role,
                });
            }

            // Not claimable: either already rotated, expired, or unknown. If it was
            // rotated within the grace window and has a still-valid successor, hand
            // that successor back (so a racing tab stays signed in instead of being
            // bounced). Otherwise the token is invalid.
            let grace = client
                .query_opt(
                    "SELECT t.handle, t.role, t.replaced_by
                     FROM refresh_tokens t
                     JOIN refresh_tokens s ON s.token = t.replaced_by
                     WHERE t.token = $1
                       AND t.replaced_by IS NOT NULL
                       AND t.rotated_at > now() - interval '30 seconds'
                       AND s.expires_at > now()",
                    &[&req.refresh_token],
                )
                .await
                .map_err(db_err)?;

            if let Some(row) = grace {
                let handle: String = row.try_get("handle").map_err(db_err)?;
                let role: String = row.try_get("role").map_err(db_err)?;
                let successor: String = row.try_get("replaced_by").map_err(db_err)?;
                let token = store.issue_jwt(&handle, &role)?;
                return Ok(TokenResponse {
                    token,
                    refresh_token: Some(successor),
                    handle,
                    role,
                });
            }

            Err(IdentityError::InvalidRefreshToken)
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
        let secret = self.secret.clone();
        async move {
            // Attribute the session first: the access token names the account
            // whose session this is, which is what lets the refresh delete be
            // scoped to that account below.
            let session_handle = req
                .token
                .as_deref()
                .and_then(|token| identity::tokens::validate_access_token(&secret, token).ok())
                .map(|validated| validated.handle);

            if let Some(token) = req.token {
                revoked
                    .lock()
                    .map_err(|e| IdentityError::Internal(e.to_string()))?
                    .insert(token);
            }
            if let Some(rt) = req.refresh_token {
                let client = db.get_client().await.map_err(db_err)?;
                match session_handle {
                    // Scoped by handle so a client that mixes up two sessions
                    // cannot sign out the wrong person. This is a guard against a
                    // confused caller, not a security boundary: whoever holds a
                    // refresh token can already mint access tokens with it, which
                    // is also why `revoke_refresh_token` needs no session at all.
                    Some(handle) => {
                        let deleted = client
                            .execute(
                                "DELETE FROM refresh_tokens WHERE token = $1 AND handle = $2",
                                &[&rt, &handle],
                            )
                            .await
                            .map_err(db_err)?;
                        if deleted == 0 {
                            // Nothing deleted: either it was already gone, which
                            // is a successful logout, or it belongs to somebody
                            // else, which is a bug in the caller and is worth
                            // saying out loud rather than silently ignoring.
                            let belongs_to_another = client
                                .query_opt("SELECT 1 FROM refresh_tokens WHERE token = $1", &[&rt])
                                .await
                                .map_err(db_err)?
                                .is_some();
                            if belongs_to_another {
                                return Err(IdentityError::InvalidRefreshToken);
                            }
                        }
                    }
                    // No usable access token to attribute the session to (absent,
                    // expired or malformed). Possession is then the only proof
                    // available, which is exactly the public method's contract.
                    None => {
                        let _ = client
                            .execute("DELETE FROM refresh_tokens WHERE token = $1", &[&rt])
                            .await;
                    }
                }
            }
            Ok(LogoutResponse {})
        }
    }

    fn revoke_refresh_token(
        &self,
        req: RevokeRefreshTokenRequest,
    ) -> impl Future<Output = Result<RevokeRefreshTokenResponse, IdentityError>> + Send {
        let db = self.db.clone();
        let refresh_token = req.refresh_token;
        async move {
            let client = db.get_client().await.map_err(db_err)?;
            // The row count is deliberately discarded: unknown, expired and
            // already-revoked tokens must be indistinguishable from a live one,
            // or this public method becomes an oracle for which tokens work.
            let _ = client
                .execute(
                    "DELETE FROM refresh_tokens WHERE token = $1",
                    &[&refresh_token],
                )
                .await
                .map_err(db_err)?;
            Ok(RevokeRefreshTokenResponse {})
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
        self.decode_jwt(token)
            .map(|c| format!("{}\x1e{}", c.sub, c.role))
    }
}

impl AccountStore for PostgresIdentityStore {
    async fn verify_password(&self, handle: &str, password: &str) -> Result<bool, IdentityError> {
        let client = self.db.get_client().await.map_err(db_err)?;
        let row = client
            .query_opt(
                "SELECT password_hash FROM users WHERE handle = $1",
                &[&handle],
            )
            .await
            .map_err(db_err)?;
        drop(client);

        let Some(row) = row else {
            return Ok(false); // unknown handle — do not leak existence
        };
        let password_hash: String = row.try_get("password_hash").map_err(db_err)?;

        let password = password.to_owned();
        tokio::task::spawn_blocking(move || {
            verify(password, &password_hash).map_err(|e| IdentityError::Internal(e.to_string()))
        })
        .await
        .map_err(|e| IdentityError::Internal(e.to_string()))?
    }

    async fn set_password(&self, handle: &str, new_password: &str) -> Result<(), IdentityError> {
        let new_password = new_password.to_owned();
        let password_hash = tokio::task::spawn_blocking(move || {
            hash(new_password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
        })
        .await
        .map_err(|e| IdentityError::Internal(e.to_string()))??;

        let affected = self
            .db
            .get_client()
            .await
            .map_err(db_err)?
            .execute(
                "UPDATE users SET password_hash = $1 WHERE handle = $2",
                &[&password_hash, &handle],
            )
            .await
            .map_err(db_err)?;
        if affected == 0 {
            return Err(IdentityError::UserNotFound);
        }
        Ok(())
    }

    async fn get_account(&self, handle: &str) -> Result<Option<AccountRecord>, IdentityError> {
        let client = self.db.get_client().await.map_err(db_err)?;
        let row = client
            .query_opt("SELECT role FROM users WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?;
        match row {
            Some(row) => {
                let role: String = row.try_get("role").map_err(db_err)?;
                Ok(Some(AccountRecord {
                    handle: handle.to_owned(),
                    role,
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_accounts(&self) -> Result<Vec<AccountRecord>, IdentityError> {
        let client = self.db.get_client().await.map_err(db_err)?;
        let rows = client
            .query("SELECT handle, role FROM users ORDER BY handle", &[])
            .await
            .map_err(db_err)?;
        rows.into_iter()
            .map(|row| {
                Ok(AccountRecord {
                    handle: row.try_get("handle").map_err(db_err)?,
                    role: row.try_get("role").map_err(db_err)?,
                })
            })
            .collect()
    }

    async fn set_role(&self, handle: &str, role: &str) -> Result<(), IdentityError> {
        let mut client = self.db.get_client().await.map_err(db_err)?;
        let tx = client.transaction().await.map_err(db_err)?;
        let affected = tx
            .execute(
                "UPDATE users SET role = $1 WHERE handle = $2",
                &[&role, &handle],
            )
            .await
            .map_err(db_err)?;
        if affected == 0 {
            return Err(IdentityError::UserNotFound);
        }
        // Same transaction as the update: a role change that left the old
        // refresh tokens behind would be refreshable straight back.
        tx.execute("DELETE FROM refresh_tokens WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn delete_account(&self, handle: &str) -> Result<(), IdentityError> {
        let mut client = self.db.get_client().await.map_err(db_err)?;
        let tx = client.transaction().await.map_err(db_err)?;
        // Explicit, not left to the schema's ON DELETE CASCADE: the other stores
        // cannot rely on theirs, and a stranded refresh token is a deleted
        // account that can still mint sessions.
        tx.execute("DELETE FROM refresh_tokens WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?;
        let affected = tx
            .execute("DELETE FROM users WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?;
        if affected == 0 {
            return Err(IdentityError::UserNotFound);
        }
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn issue_session(&self, handle: &str) -> Result<TokenResponse, IdentityError> {
        let client = self.db.get_client().await.map_err(db_err)?;
        let row = client
            .query_opt("SELECT role FROM users WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?
            .ok_or(IdentityError::UserNotFound)?;
        let role: String = row.try_get("role").map_err(db_err)?;

        // Release the pooled connection before issue_refresh_token acquires its own,
        // or concurrent callers deadlock the pool (see login).
        drop(row);
        drop(client);

        let token = self.issue_jwt(handle, &role)?;
        let refresh_token = self.issue_refresh_token(handle, &role).await?;
        Ok(TokenResponse {
            token,
            refresh_token: Some(refresh_token),
            handle: handle.to_owned(),
            role,
        })
    }

    async fn revoke_sessions(&self, handle: &str) -> Result<(), IdentityError> {
        self.db
            .get_client()
            .await
            .map_err(db_err)?
            .execute("DELETE FROM refresh_tokens WHERE handle = $1", &[&handle])
            .await
            .map_err(db_err)?;
        Ok(())
    }
}
