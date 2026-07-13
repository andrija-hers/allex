use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use message_templates::{
    ForgotPasswordMessageRequest, ForgotPasswordMessageResponse, RenderMessageTemplateRequest,
    RenderedMessageResponse, TemplateChannel, TemplateError, UpsertMessageTemplateRequest,
    render_stored_template,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use transport::BearerTokenValidator;

use crate::{
    AccountRecord, AccountStore, IdentityError, IdentityV1, LoginRequest, LogoutRequest,
    LogoutResponse, RefreshRequest, RegisterRequest, RoleResponse, TokenResponse, ValidateRequest,
};

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

struct StoredUser {
    password_hash: String,
    role: String,
}

struct IdentityStoreInner {
    secret: String,
    token_ttl_secs: u64,
    refresh_token_ttl_secs: u64,
    users: Arc<Mutex<HashMap<String, StoredUser>>>,
    revoked_tokens: Arc<Mutex<HashSet<String>>>,
    // refresh_token -> (handle, role, expiry_unix_secs)
    refresh_tokens: Arc<Mutex<HashMap<String, (String, String, u64)>>>,
    // (template_key, channel) -> template body
    templates: Arc<Mutex<HashMap<(String, TemplateChannel), String>>>,
}

/// An in-memory identity store supporting register, login, JWT validation, and logout.
#[derive(Clone)]
pub struct InMemoryIdentityStore(Arc<IdentityStoreInner>);

impl InMemoryIdentityStore {
    /// Creates a new in-memory store that signs and validates JWTs with the given secret.
    pub fn new(secret: impl Into<String>) -> Self {
        Self::new_with_ttl(secret, 3600, 7 * 24 * 3600)
    }

    pub fn new_with_ttl(
        secret: impl Into<String>,
        token_ttl_secs: u64,
        refresh_token_ttl_secs: u64,
    ) -> Self {
        Self(Arc::new(IdentityStoreInner {
            secret: secret.into(),
            token_ttl_secs,
            refresh_token_ttl_secs,
            users: Arc::new(Mutex::new(HashMap::new())),
            revoked_tokens: Arc::new(Mutex::new(HashSet::new())),
            refresh_tokens: Arc::new(Mutex::new(HashMap::new())),
            templates: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    pub async fn upsert_message_template(
        &self,
        req: UpsertMessageTemplateRequest,
    ) -> Result<(), TemplateError> {
        self.0
            .templates
            .lock()
            .await
            .insert((req.template_key, req.channel), req.body);
        Ok(())
    }

    pub async fn render_message_template(
        &self,
        req: RenderMessageTemplateRequest,
    ) -> Result<RenderedMessageResponse, TemplateError> {
        let template = self
            .0
            .templates
            .lock()
            .await
            .get(&(req.template_key, req.channel))
            .cloned()
            .ok_or(TemplateError::NotFound)?;
        let message = render_stored_template(&template, &req.data)?;
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
            &DecodingKey::from_secret(self.0.secret.as_bytes()),
            &Validation::default(),
        )
        .ok()
        .map(|d| d.claims)
    }

    fn issue_jwt(&self, handle: &str, role: &str) -> Result<String, IdentityError> {
        let exp = now_secs() + self.0.token_ttl_secs as usize;
        let claims = Claims {
            sub: handle.to_owned(),
            role: role.to_owned(),
            exp,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.0.secret.as_bytes()),
        )
        .map_err(|e| IdentityError::Internal(e.to_string()))
    }

    async fn issue_refresh_token(&self, handle: &str, role: &str) -> String {
        let token = new_opaque_token();
        let expiry = now_secs() as u64 + self.0.refresh_token_ttl_secs;
        self.0
            .refresh_tokens
            .lock()
            .await
            .insert(token.clone(), (handle.to_owned(), role.to_owned(), expiry));
        token
    }
}

fn now_secs() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
}

fn new_opaque_token() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Simple but sufficient for in-memory testing: random-ish via thread RNG substitute
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let tid = std::thread::current().id();
    format!("rt-{ns}-{tid:?}")
}

impl IdentityV1 for InMemoryIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let inner = self.0.clone();
        let store = self.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let mut users = inner.users.lock().await;
            if users.contains_key(&req.handle) {
                return Err(IdentityError::HandleAlreadyTaken);
            }
            users.insert(
                req.handle.clone(),
                StoredUser {
                    password_hash,
                    role: req.role.clone(),
                },
            );
            drop(users);

            let token = store.issue_jwt(&req.handle, &req.role)?;
            let refresh_token = store.issue_refresh_token(&req.handle, &req.role).await;
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
        let inner = self.0.clone();
        let store = self.clone();
        async move {
            let (password_hash, role) = {
                let users = inner.users.lock().await;
                match users.get(&req.handle) {
                    Some(u) => (u.password_hash.clone(), u.role.clone()),
                    None => return Err(IdentityError::InvalidCredentials),
                }
            };

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
            let refresh_token = store.issue_refresh_token(&req.handle, &role).await;
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
        let inner = self.0.clone();
        let store = self.clone();
        async move {
            let (handle, role) = {
                let mut refresh_tokens = inner.refresh_tokens.lock().await;
                match refresh_tokens.get(&req.refresh_token) {
                    Some((h, r, expiry)) if *expiry > now_secs() as u64 => (h.clone(), r.clone()),
                    _ => {
                        refresh_tokens.remove(&req.refresh_token);
                        return Err(IdentityError::InvalidRefreshToken);
                    }
                }
            };

            // Rotate: remove old refresh token, issue new pair
            inner.refresh_tokens.lock().await.remove(&req.refresh_token);
            let token = store.issue_jwt(&handle, &role)?;
            let new_refresh = store.issue_refresh_token(&handle, &role).await;
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
        let secret = self.0.secret.clone();
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
        let revoked = self.0.revoked_tokens.clone();
        let refresh_tokens = self.0.refresh_tokens.clone();
        async move {
            if let Some(token) = req.token {
                revoked.lock().await.insert(token);
            }
            if let Some(rt) = req.refresh_token {
                refresh_tokens.lock().await.remove(&rt);
            }
            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for InMemoryIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        let is_revoked = self
            .0
            .revoked_tokens
            .try_lock()
            .map(|set| set.contains(token))
            .unwrap_or(false);
        if is_revoked {
            return None;
        }
        self.decode_jwt(token)
            .map(|c| format!("{}\x1e{}", c.sub, c.role))
    }
}

impl AccountStore for InMemoryIdentityStore {
    async fn verify_password(&self, handle: &str, password: &str) -> Result<bool, IdentityError> {
        let stored_hash = {
            let users = self.0.users.lock().await;
            match users.get(handle) {
                Some(u) => u.password_hash.clone(),
                None => return Ok(false),
            }
        };
        let password = password.to_owned();
        tokio::task::spawn_blocking(move || {
            verify(password, &stored_hash).map_err(|e| IdentityError::Internal(e.to_string()))
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

        let mut users = self.0.users.lock().await;
        match users.get_mut(handle) {
            Some(u) => {
                u.password_hash = password_hash;
                Ok(())
            }
            None => Err(IdentityError::UserNotFound),
        }
    }

    async fn get_account(&self, handle: &str) -> Result<Option<AccountRecord>, IdentityError> {
        let users = self.0.users.lock().await;
        Ok(users.get(handle).map(|u| AccountRecord {
            handle: handle.to_owned(),
            role: u.role.clone(),
        }))
    }

    async fn issue_session(&self, handle: &str) -> Result<TokenResponse, IdentityError> {
        let role = {
            let users = self.0.users.lock().await;
            match users.get(handle) {
                Some(u) => u.role.clone(),
                None => return Err(IdentityError::UserNotFound),
            }
        };
        let token = self.issue_jwt(handle, &role)?;
        let refresh_token = self.issue_refresh_token(handle, &role).await;
        Ok(TokenResponse {
            token,
            refresh_token: Some(refresh_token),
            handle: handle.to_owned(),
            role,
        })
    }

    async fn revoke_sessions(&self, handle: &str) -> Result<(), IdentityError> {
        self.0
            .refresh_tokens
            .lock()
            .await
            .retain(|_, (h, _, _)| h != handle);
        Ok(())
    }
}

#[cfg(test)]
mod account_store_tests {
    use super::*;

    async fn store_with_user(handle: &str, password: &str, role: &str) -> InMemoryIdentityStore {
        let store = InMemoryIdentityStore::new("test-secret");
        store
            .register(RegisterRequest {
                handle: handle.into(),
                password: password.into(),
                role: role.into(),
            })
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn verify_password_matches_only_the_correct_password() {
        let store = store_with_user("alice", "pw", "user").await;
        assert!(store.verify_password("alice", "pw").await.unwrap());
        assert!(!store.verify_password("alice", "wrong").await.unwrap());
        // Unknown handle: Ok(false), not an error — existence is not leaked.
        assert!(!store.verify_password("nobody", "pw").await.unwrap());
    }

    #[tokio::test]
    async fn set_password_changes_login_and_reports_unknown_user() {
        let store = store_with_user("bob", "old", "user").await;
        store.set_password("bob", "new").await.unwrap();
        assert!(!store.verify_password("bob", "old").await.unwrap());
        assert!(store.verify_password("bob", "new").await.unwrap());
        assert!(
            store
                .login(LoginRequest {
                    handle: "bob".into(),
                    password: "new".into(),
                })
                .await
                .is_ok()
        );
        assert!(matches!(
            store.set_password("ghost", "x").await,
            Err(IdentityError::UserNotFound)
        ));
    }

    #[tokio::test]
    async fn get_account_returns_role_or_none() {
        let store = store_with_user("carol", "pw", "admin").await;
        let record = store.get_account("carol").await.unwrap().unwrap();
        assert_eq!(record.handle, "carol");
        assert_eq!(record.role, "admin");
        assert!(store.get_account("nobody").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn issue_session_mints_a_valid_usable_session() {
        let store = store_with_user("dave", "pw", "user").await;
        let session = store.issue_session("dave").await.unwrap();
        assert_eq!(session.handle, "dave");
        assert_eq!(session.role, "user");
        // The access token validates through the normal path...
        let validated = store
            .validate(ValidateRequest {
                token: session.token.clone(),
            })
            .await
            .unwrap();
        assert_eq!(validated.role, "user");
        // ...and the refresh token works through the normal refresh path.
        let refreshed = store
            .refresh(RefreshRequest {
                refresh_token: session.refresh_token.clone().unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(refreshed.handle, "dave");
        assert!(matches!(
            store.issue_session("ghost").await,
            Err(IdentityError::UserNotFound)
        ));
    }

    #[tokio::test]
    async fn revoke_sessions_invalidates_existing_refresh_tokens() {
        let store = store_with_user("erin", "pw", "user").await;
        let session = store.issue_session("erin").await.unwrap();
        let refresh_token = session.refresh_token.clone().unwrap();
        store.revoke_sessions("erin").await.unwrap();
        assert!(matches!(
            store.refresh(RefreshRequest { refresh_token }).await,
            Err(IdentityError::InvalidRefreshToken)
        ));
    }
}
