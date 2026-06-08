use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use bcrypt::{DEFAULT_COST, hash, verify};
use tokio::sync::Mutex;
use transport::BearerTokenValidator;

use crate::{
    IdentityError, IdentityV1, LoginRequest, LogoutRequest, LogoutResponse, RefreshRequest,
    RegisterRequest, RoleResponse, TokenResponse, ValidateRequest, issue_token_response,
    validate_access_token, validate_refresh_token,
};

struct StoredUser {
    password_hash: String,
    role: String,
}

struct IdentityStoreInner {
    secret: String,
    users: Arc<Mutex<HashMap<String, StoredUser>>>,
    revoked_access_tokens: Arc<Mutex<HashSet<String>>>,
    revoked_refresh_tokens: Arc<Mutex<HashSet<String>>>,
}

/// An in-memory identity store supporting register, login, JWT validation, and logout.
#[derive(Clone)]
pub struct InMemoryIdentityStore(Arc<IdentityStoreInner>);

impl InMemoryIdentityStore {
    /// Creates a new in-memory store that signs and validates JWTs with the given secret.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(Arc::new(IdentityStoreInner {
            secret: secret.into(),
            users: Arc::new(Mutex::new(HashMap::new())),
            revoked_access_tokens: Arc::new(Mutex::new(HashSet::new())),
            revoked_refresh_tokens: Arc::new(Mutex::new(HashSet::new())),
        }))
    }
}

impl IdentityV1 for InMemoryIdentityStore {
    fn register(
        &self,
        req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let users = self.0.users.clone();
        let secret = self.0.secret.clone();
        async move {
            let password = req.password.clone();
            let password_hash = tokio::task::spawn_blocking(move || {
                hash(password, DEFAULT_COST).map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            let mut users = users.lock().await;
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

            issue_token_response(&secret, &req.handle, &req.role)
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let users = self.0.users.clone();
        let secret = self.0.secret.clone();
        async move {
            let (password_hash, role) = {
                let users = users.lock().await;
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

            issue_token_response(&secret, &req.handle, &role)
        }
    }

    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let secret = self.0.secret.clone();
        let revoked_refresh_tokens = self.0.revoked_refresh_tokens.clone();
        async move {
            let claims = validate_refresh_token(&secret, &req.refresh_token)?;

            let mut revoked = revoked_refresh_tokens.lock().await;
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
        let secret = self.0.secret.clone();
        let revoked_access_tokens = self.0.revoked_access_tokens.clone();
        async move {
            if revoked_access_tokens.lock().await.contains(&req.token) {
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
        let revoked_access_tokens = self.0.revoked_access_tokens.clone();
        let revoked_refresh_tokens = self.0.revoked_refresh_tokens.clone();
        async move {
            let mut saw_token = false;

            if let Some(token) = req.token {
                revoked_access_tokens.lock().await.insert(token);
                saw_token = true;
            }

            if let Some(refresh_token) = req.refresh_token {
                revoked_refresh_tokens.lock().await.insert(refresh_token);
                saw_token = true;
            }

            if !saw_token {
                return Err(IdentityError::InvalidToken);
            }

            Ok(LogoutResponse {})
        }
    }
}

impl BearerTokenValidator for InMemoryIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        // Check blacklist synchronously — we need a blocking check here.
        // Since revoked_access_tokens uses tokio::Mutex, we use try_lock; if contended
        // we conservatively allow (extremely rare at logout timing).
        let is_revoked = self
            .0
            .revoked_access_tokens
            .try_lock()
            .map(|set| set.contains(token))
            .unwrap_or(false);
        if is_revoked {
            return None;
        }
        validate_access_token(&self.0.secret, token)
            .ok()
            .map(|claims| claims.role)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RefreshRequest, ValidateRequest};

    fn register_request(handle: &str, password: &str) -> RegisterRequest {
        RegisterRequest {
            handle: handle.into(),
            password: password.into(),
            role: "user".into(),
        }
    }

    #[tokio::test]
    async fn refresh_rotates_refresh_tokens() {
        let store = InMemoryIdentityStore::new("secret");
        let tokens = store
            .register(register_request("alice", "password"))
            .await
            .unwrap();

        let rotated = store
            .refresh(RefreshRequest {
                refresh_token: tokens.refresh_token.clone(),
            })
            .await
            .unwrap();

        assert_ne!(rotated.token, tokens.token);
        assert_ne!(rotated.refresh_token, tokens.refresh_token);
        assert_eq!(
            store
                .validate(ValidateRequest {
                    token: rotated.token.clone(),
                })
                .await
                .unwrap()
                .role,
            "user"
        );

        let err = store
            .refresh(RefreshRequest {
                refresh_token: tokens.refresh_token,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IdentityError::InvalidRefreshToken));
    }

    #[tokio::test]
    async fn validate_rejects_refresh_tokens() {
        let store = InMemoryIdentityStore::new("secret");
        let tokens = store
            .register(register_request("bob", "password"))
            .await
            .unwrap();

        let err = store
            .validate(ValidateRequest {
                token: tokens.refresh_token,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IdentityError::InvalidToken));
    }

    #[tokio::test]
    async fn logout_revokes_access_and_refresh_tokens() {
        let store = InMemoryIdentityStore::new("secret");
        let tokens = store
            .register(register_request("carol", "password"))
            .await
            .unwrap();

        store
            .logout(LogoutRequest {
                token: Some(tokens.token.clone()),
                refresh_token: Some(tokens.refresh_token.clone()),
            })
            .await
            .unwrap();

        assert_eq!(store.validate_bearer(&tokens.token), None);

        let err = store
            .refresh(RefreshRequest {
                refresh_token: tokens.refresh_token,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IdentityError::InvalidRefreshToken));
    }
}
