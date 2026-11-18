# allex-identity

> The IdentityV1 service, JWT engine, and pluggable user seams.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

The identity layer for allex: the `IdentityV1` service (`register` / `login` /
`refresh` / `validate` / `logout`), a reusable JWT engine (`identity::tokens`),
and the seams for plugging in your own users. Pick your entry point:

- **Stock `{handle, password, role}` users** → use a store from
  `allex-identity-sqlite` / `-postgres` / `-redb` (or `InMemoryIdentityStore` for
  dev). Seed the first account with `ensure_user`.
- **Custom credential source or richer rights** → implement `CredentialBackend`
  and wrap it in `CredentialStore<B>`; it carries an opaque per-user **profile**
  (e.g. JSON rights) in the token, surfaced as `caller_profile()`.
- **Account management around login** (change a password, mint a session for an
  SSO/passkey flow) → the `AccountStore` trait, implemented by every stock store.
- **Public signup** → wrap the store in `RegistrationGuard::force_role(store,
  "user")` so `register` can't self-elevate to a privileged role.

See `AGENTS.md` for the full decision guide and the profile-claim caveats (the
profile is signed, not encrypted; keep it small; rights refresh at the token
boundary).

## Docs & license

API documentation: **[docs.rs/allex-identity](https://docs.rs/allex-identity)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
