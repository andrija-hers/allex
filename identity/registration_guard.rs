//! [`RegistrationGuard`] — constrain what role public self-registration may assign.
//!
//! The default [`IdentityV1::register`](crate::IdentityV1::register) trusts the role in the request,
//! so an open signup endpoint would let anyone mint an `admin`. Wrapping a store in a
//! [`RegistrationGuard`] fixes that in one line: it delegates **every** method (and
//! `BearerTokenValidator` / [`AccountStore`]) to the inner store unchanged, except `register`, where
//! it applies a role policy. Provision privileged accounts out of band with
//! [`ensure_user`](crate::ensure_user), not through public signup.

use std::collections::BTreeSet;
use std::future::Future;

use transport::BearerTokenValidator;

use crate::{
    AccountRecord, AccountStore, IdentityError, IdentityV1, LoginRequest, LogoutRequest,
    LogoutResponse, RefreshRequest, RegisterRequest, RevokeRefreshTokenRequest,
    RevokeRefreshTokenResponse, RoleResponse, TokenResponse, ValidateRequest,
};

#[derive(Clone, Debug)]
enum RolePolicy {
    Force(String),
    Allow(BTreeSet<String>),
}

/// Wraps an [`IdentityV1`] store and constrains the role `register` may assign, so public
/// self-registration can't create a privileged account. Everything else — `login` / `refresh` /
/// `validate` / `logout`, `BearerTokenValidator`, and [`AccountStore`] — is delegated to the inner
/// store unchanged, so a guarded store is a drop-in replacement for the raw one.
#[derive(Clone)]
pub struct RegistrationGuard<I> {
    inner: I,
    policy: RolePolicy,
}

impl<I> RegistrationGuard<I> {
    /// Every `register` is assigned `role`, **ignoring** whatever role the client requested. The
    /// common case — "public signup is always a plain user; admins are seeded out of band".
    pub fn force_role(inner: I, role: impl Into<String>) -> Self {
        Self {
            inner,
            policy: RolePolicy::Force(role.into()),
        }
    }

    /// `register` may only request a role in `allowed`; any other role is rejected with
    /// [`IdentityError::InvalidCredentials`]. Use when several self-service roles are legitimate.
    pub fn allow_roles(inner: I, allowed: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            inner,
            policy: RolePolicy::Allow(allowed.into_iter().map(Into::into).collect()),
        }
    }

    /// The wrapped store, for operations that should bypass the guard (e.g. account management).
    pub fn inner(&self) -> &I {
        &self.inner
    }

    fn apply_policy(&self, req: &mut RegisterRequest) -> Result<(), IdentityError> {
        match &self.policy {
            RolePolicy::Force(role) => {
                req.role = role.clone();
                Ok(())
            }
            RolePolicy::Allow(allowed) => {
                if allowed.contains(&req.role) {
                    Ok(())
                } else {
                    Err(IdentityError::InvalidCredentials)
                }
            }
        }
    }
}

impl<I: IdentityV1 + Send + Sync> IdentityV1 for RegistrationGuard<I> {
    fn register(
        &self,
        mut req: RegisterRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        let policy = self.apply_policy(&mut req);
        async move {
            policy?;
            self.inner.register(req).await
        }
    }

    fn login(
        &self,
        req: LoginRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        self.inner.login(req)
    }

    fn refresh(
        &self,
        req: RefreshRequest,
    ) -> impl Future<Output = Result<TokenResponse, IdentityError>> + Send {
        self.inner.refresh(req)
    }

    fn validate(
        &self,
        req: ValidateRequest,
    ) -> impl Future<Output = Result<RoleResponse, IdentityError>> + Send {
        self.inner.validate(req)
    }

    fn logout(
        &self,
        req: LogoutRequest,
    ) -> impl Future<Output = Result<LogoutResponse, IdentityError>> + Send {
        self.inner.logout(req)
    }

    fn revoke_refresh_token(
        &self,
        req: RevokeRefreshTokenRequest,
    ) -> impl Future<Output = Result<RevokeRefreshTokenResponse, IdentityError>> + Send {
        self.inner.revoke_refresh_token(req)
    }
}

impl<I: BearerTokenValidator> BearerTokenValidator for RegistrationGuard<I> {
    fn validate_bearer(&self, token: &str) -> Option<String> {
        self.inner.validate_bearer(token)
    }
}

impl<I: AccountStore + Send + Sync> AccountStore for RegistrationGuard<I> {
    async fn verify_password(&self, handle: &str, password: &str) -> Result<bool, IdentityError> {
        self.inner.verify_password(handle, password).await
    }

    async fn set_password(&self, handle: &str, new_password: &str) -> Result<(), IdentityError> {
        self.inner.set_password(handle, new_password).await
    }

    async fn get_account(&self, handle: &str) -> Result<Option<AccountRecord>, IdentityError> {
        self.inner.get_account(handle).await
    }

    async fn issue_session(&self, handle: &str) -> Result<TokenResponse, IdentityError> {
        self.inner.issue_session(handle).await
    }

    async fn revoke_sessions(&self, handle: &str) -> Result<(), IdentityError> {
        self.inner.revoke_sessions(handle).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryIdentityStore;

    #[tokio::test]
    async fn force_role_overrides_a_client_supplied_privileged_role() {
        let guard = RegistrationGuard::force_role(InMemoryIdentityStore::new("secret"), "user");
        // Client asks for "admin"; the guard pins it to "user".
        let tokens = guard
            .register(RegisterRequest {
                handle: "mallory".into(),
                password: "pw".into(),
                role: "admin".into(),
            })
            .await
            .unwrap();
        assert_eq!(tokens.role, "user");
        // The account really is a "user" in the underlying store.
        let record = guard.inner().get_account("mallory").await.unwrap().unwrap();
        assert_eq!(record.role, "user");
    }

    #[tokio::test]
    async fn allow_roles_accepts_listed_and_rejects_others() {
        let guard = RegistrationGuard::allow_roles(
            InMemoryIdentityStore::new("secret"),
            ["user", "seller"],
        );
        assert!(
            guard
                .register(RegisterRequest {
                    handle: "sam".into(),
                    password: "pw".into(),
                    role: "seller".into(),
                })
                .await
                .is_ok()
        );
        assert!(matches!(
            guard
                .register(RegisterRequest {
                    handle: "eve".into(),
                    password: "pw".into(),
                    role: "admin".into(),
                })
                .await,
            Err(IdentityError::InvalidCredentials)
        ));
    }

    #[tokio::test]
    async fn other_methods_are_delegated_unchanged() {
        let guard = RegistrationGuard::force_role(InMemoryIdentityStore::new("secret"), "user");
        guard
            .register(RegisterRequest {
                handle: "carol".into(),
                password: "pw".into(),
                role: "user".into(),
            })
            .await
            .unwrap();
        // login (delegated) succeeds and the bearer validator (delegated) accepts the token.
        let tokens = guard
            .login(LoginRequest {
                handle: "carol".into(),
                password: "pw".into(),
            })
            .await
            .unwrap();
        assert!(guard.validate_bearer(&tokens.token).is_some());
    }
}
