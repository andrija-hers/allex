use std::{collections::HashMap, future::Future, sync::Arc};

use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use transport::BearerTokenValidator;

use crate::{
    IdentityError, IdentityV1, LoginRequest, RegisterRequest, RoleResponse, TokenResponse,
    ValidateRequest,
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
    users: Arc<Mutex<HashMap<String, StoredUser>>>,
}

/// An in-memory identity store supporting register, login, and JWT validation.
#[derive(Clone)]
pub struct InMemoryIdentityStore(Arc<IdentityStoreInner>);

impl InMemoryIdentityStore {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(Arc::new(IdentityStoreInner {
            secret: secret.into(),
            users: Arc::new(Mutex::new(HashMap::new())),
        }))
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

            let exp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as usize
                + 3600;
            let claims = Claims {
                sub: req.handle,
                role: req.role,
                exp,
            };
            let token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(secret.as_bytes()),
            )
            .map_err(|e| IdentityError::Internal(e.to_string()))?;
            Ok(TokenResponse { token })
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
                verify(password, &password_hash)
                    .map_err(|e| IdentityError::Internal(e.to_string()))
            })
            .await
            .map_err(|e| IdentityError::Internal(e.to_string()))??;

            if !valid {
                return Err(IdentityError::InvalidCredentials);
            }

            let exp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as usize
                + 3600;
            let claims = Claims {
                sub: req.handle,
                role,
                exp,
            };
            let token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(secret.as_bytes()),
            )
            .map_err(|e| IdentityError::Internal(e.to_string()))?;
            Ok(TokenResponse { token })
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
}

impl BearerTokenValidator for InMemoryIdentityStore {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        self.decode_jwt(token).map(|c| c.role)
    }
}
