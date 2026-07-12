# allex — orientation for a new agent (or human)

`allex` is a Rust workspace of `allex-*` crates for building **RPC services**: you
declare a service as a trait, a proc-macro generates the client + server glue +
typed clients for other languages, and a set of runtime crates handle transport,
dispatch, identity, streaming, and state. This file is the map. There is no other
top-level doc — read the crate-level `//!` comments for detail.

## The five load-bearing facts (these trip people up)

1. **A service is a `#[rpc_module]` trait.** `allex-rpc-macros` turns
   `#[rpc_module] trait FooV1 { ... }` into `FooV1Client`, `FooV1MethodResponder`
   (server side), and `FooV1MethodPermissions`. Methods return
   `impl Future<Output = Result<T, E>> + Send`; streaming methods return a
   `ProgressableFuture` (see `allex-progressable-future`).

2. **Authorization is attributes on the trait, enforced in generated code.**
   `#[public]` = anyone (incl. anonymous); `#[allowed_roles("admin","user")]` =
   those roles only. The generated responder calls
   `FooV1MethodPermissions::is_authorized(method, caller_role)` **before** decoding
   the request and returns `Error::AccessForbidden` if it fails. You never write a
   manual role check.

3. **`allex-guards` is NOT authorization.** It is a disk/RAM headroom watchdog
   (gauges + hysteresis for fire-and-forget services). Auth lives in
   `rpc-macros` + `serve` + `transport` + `identity`. Don't reach for `guards`
   when you mean rights.

4. **The caller is an ambient task-local, not a handler argument.**
   `allex-serve` installs a `CallerContext { handle, role, profile }` per request;
   read it inside a method with `allex_serve::caller_handle()` / `caller_role()` /
   `caller_profile()`. The wire form is `handle\x1erole[\x1eprofile]` (0x1E record
   separator), produced by a store's `BearerTokenValidator::validate_bearer`.

5. **Error → HTTP status** (axum transport): success = **200** (or
   `application/x-ndjson` for a stream); application error (`Err(YourError)`,
   i.e. `transport::Error::App`) = **422**; failed role check
   (`Error::AccessForbidden`) = **403**; bad request body = **400**; transport
   fault = **500**. **There is no 401** — 403 is authz, 422 is a domain/credential
   error. For **per-object** authz inside a unary method (beyond the coarse
   `#[allowed_roles]` gate — "only the creator may edit this"), call
   `allex_serve::deny_access()` (or `allex_serve::authorize(cond, err)?`) so the
   denial becomes a **403**, not a 422 — otherwise a domain `Err` reads as an
   application error.

## Crate menu

**Contract / codegen**
- `rpc-macro` (`allex-rpc-macros`) — `#[rpc_module]` / `#[allowed_roles]` /
  `#[public]`; generates client, responder, and permission helpers. **Idea:** one
  trait *is* the whole contract; method **return type** (not an attribute) picks
  unary vs streaming; wire name splits the version (`FooV1` → `foo_v_1`). See the
  crate `//!`.
- `dart-codegen`, `ts-codegen` — generate Dart / TypeScript clients from a
  `#[rpc_module]` source file. **Idea:** they parse Rust *source* (not macro output),
  emit only DTOs *reachable* from method signatures, and remap to the exact serde JSON
  wire keys (`rename_all` honored on struct fields **and** enum variants) so all
  language clients are byte-compatible. See the crate `//!`.

**Runtime**
- `transport` (`allex-transport`) — wire types (`Payload`, `Error`,
  `ModuleMethodCall`), codecs (`JsonCodec`), `AxumTransport`
  (`router` / `router_with_auth`), plus reqwest / signalr / websocket transports;
  defines `BearerTokenValidator`. **Idea:** payloads are opaque bytes, the codec is a
  *type* not a value, `Error::App` is the transport-vs-app-error seam, and this crate
  is where the error→HTTP-status table (fact 5) is actually enforced. See the crate `//!`.
- `execution` (`allex-execution`) — `Registry` + `ModuleExecutor` /
  `MethodExecutor` / `StreamingMethodExecutor` dispatch traits. **Idea:** the routing
  seam; `Registry::register` wires unary + streaming at once, and the `Response`→`Result`
  flattening is *where* app-error (422) vs transport-error (403) is decided. See the crate `//!`.
- `serve` (`allex-serve`) — `serve_dispatch` (drains a transport, spawns a task
  per request) + the `CallerContext` task-local (`caller_handle`/`caller_role`/
  `caller_profile`) + `deny_access()`/`authorize()` for per-object 403s.
  Deliberately transport-agnostic.
- `host` (`allex-host`) — the `ServiceHost` builder: batteries-included axum host
  that owns the `Registry` + `AxumTransport` + `serve_dispatch` wiring (and its
  spawn footgun) and hands back an extensible `axum::Router`. **Start here for a new
  backend.** See the crate `//!`.
- `progressable-future` (`allex-progressable-future`) — the streaming-RPC future
  and step model. **Idea:** a future that emits `Progress(P)*` then one terminal
  `Success`/`Error`/`Transport` (`ProgressOutcome`); consume via `.stream()` /
  `.await_result()`. Substrate under every streaming method. See the crate `//!`.
- `state` / `state-macro` (`allex-state`) — `LocalState` snapshot-delta streams.
- `scheduler` (`allex-scheduler`) — scheduled/recurring tasks. **Idea:** a *pure tick
  engine*, not a timer — no clock/threads; you pass "now", it tells you what should have
  fired. DST-correct. See the crate `//!`.
- `winsvc` (`allex-winsvc`) — Windows service host wrapper.

**Identity** (see the dedicated section below)
- `identity` (`allex-identity`), `identity-sqlite`, `identity-postgres`,
  `identity-redb`.

**Data / util**
- `postgres` / `postgres-macros` (`allex-postgres`) — pooled Postgres access +
  a `FromRow` derive. **Idea:** `Db` wraps a deadpool pool (`with_client`); the crate
  re-exports `tokio_postgres`/`deadpool_postgres` so dependents (and `FromRow` output)
  name those types without direct deps. See the crate `//!`s.
- `message-templates` (`allex-message-templates`) — stored message templates
  (tera rendering). **Idea:** a *leaf of pure contract types* + one render helper —
  not a service or store (storage is the consumer's job). `mailgunner`
  (`allex-mailgunner`) — Mailgun sending.
- `awaitable-map`, `keyed-events`, `watch-singleton`, `script` — async map,
  keyed event bus, single-instance guard, and scripting helpers respectively.
  (`watch-singleton` **idea:** a `'static` hot-reloadable config singleton with an
  `Apply`/`register` push-on-change pattern — see its crate `//!`.)

## Identity — pick the right entry point

allex ships the `IdentityV1` service (`register` / `login` / `refresh` /
`validate` / `logout`) plus four ready-made stores:
`InMemoryIdentityStore` (dev/tests), `SqliteIdentityStore`, `PostgresIdentityStore`,
`RedbIdentityStore`. All do **bcrypt over a `users(handle, password_hash, role)`
table** and implement `BearerTokenValidator`.

Decision guide:
- **Your users fit `{handle, password}` + a single role, bcrypt-hashed** → use a
  stock store directly. Seed the first account with
  `identity::ensure_user(&store, handle, password, role)` (idempotent) — as
  epicfix's `fix-svc-host` does for its admin operator (the reference consumer).
- **Custom credential source or rights beyond one role** (external DB, a stored
  proc, an SSO guid, per-user permission flags) → do **not** hand-write a fifth
  store. Implement `identity::CredentialBackend` (just `verify_credentials` +
  `resolve_user`) and wrap it in `identity::CredentialStore<B>`, which provides
  `login` / `refresh` / `logout` + `BearerTokenValidator` on the shared
  `identity::tokens` JWT engine. It carries an opaque per-user **profile**
  (e.g. a JSON string of rights) in the token and re-emits it as the third
  `\x1e` segment → `allex_serve::caller_profile()`. `refresh` re-resolves the
  user, so rights changes propagate at the refresh boundary. Session state is
  in-memory in this version (single-instance).
- `identity::tokens` is the reusable JWT engine (issue/validate access & refresh
  tokens, optional opaque profile claim). Use it if you build a store by hand.
- **Managing accounts *around* login** — change/verify a password, read a user, or
  mint a session for a passkey/SSO/custom login that authenticated the user **out of
  band** → the `identity::AccountStore` trait, implemented by **every** stock store:
  `verify_password` / `set_password` / `get_account` / `issue_session` /
  `revoke_sessions`. **Never reach into a store's `users` / `refresh_tokens` tables
  with raw SQL, and never hand-mint a JWT** — that couples you to the store's private
  schema, bcrypt cost, and token format, and silently breaks when they change.
  `issue_session(handle)` hands a passkey/SSO flow the *exact* token pair `login`
  returns (role re-read, refresh token persisted), so `refresh` / `logout` /
  `validate_bearer` all work unchanged. Pair `set_password` with `revoke_sessions`
  when a password change should log other devices out.
- **Public signup must not self-elevate** — a raw `register` trusts the client's
  requested role, so an open endpoint lets anyone mint an `admin`. Wrap the store in
  `identity::RegistrationGuard::force_role(store, "user")` (or `allow_roles([..])`)
  before registering it as the `IdentityV1` module: it pins/limits the role `register`
  assigns and delegates every other method (`BearerTokenValidator` / `AccountStore`
  included). Provision privileged accounts out of band with `ensure_user`, never via
  signup. (Only needed if a privileged role exists — a service whose methods are all
  one role has nothing to escalate to.)

### `CredentialStore` / profile claims — words of wisdom

Learned while building this; ignore at your peril.

- **The profile is opaque end-to-end, and it is a `String` on purpose.** allex
  never parses it — not in the token, not in `validate_bearer`, not in
  `CallerContext`. Serialize your rights struct to JSON at the *store* boundary
  (in `AuthenticatedUser.profile`) and deserialize it at the *method* boundary
  (`serde_json::from_str(&caller_profile()?)`). Don't try to make allex generic
  over your rights type; the `String` seam is the whole point.
- **It rides in every request's JWT — keep it small.** The profile is part of the
  signed access token sent on every call. Put a compact rights bitset / a few
  booleans there, not a fat user record. Big profile = big header on every RPC.
- **A JWT is signed, not encrypted — the client can read the profile.** Only put
  authorization data the user is allowed to see (rights flags are fine; server
  re-checks them anyway). Never secrets.
- **`validate_bearer` is synchronous and on the hot path.** It runs per request.
  The revocation check uses a non-blocking `try_lock`; never put an `await` or a
  DB round-trip in there. The profile it emits comes straight from the token, so
  it needs no backend lookup — keep it that way.
- **Rights freshness is a deliberate trade, not an accident.** Rights are frozen
  into the access token at issue time. `CredentialStore::refresh` calls
  `resolve_user`, so a refresh re-reads current rights → changes propagate at the
  refresh boundary (staleness ≤ the access-token TTL). **Do not shrink the global
  access TTL just to revoke faster** — that only raises refresh traffic and the
  refresh-token rotation-race for multi-tab clients. For a right that must be
  revoked *instantly* (destructive/admin actions), do a per-request fresh check
  inside that method instead of trusting the token.
- **`resolve_user` runs on every refresh — make it cheap.** Cache it if your
  backend is slow (it's the price of freshness).
- **`verify_credentials`: `Ok(None)` = wrong password, `Err` = backend fault.**
  Don't return `Err` for bad credentials — the store maps `Ok(None)` to
  `InvalidCredentials`. Reserve `Err` for "the DB is down."
- **Session state is in-memory ⇒ effectively single-instance.** Refresh tokens
  and the revocation set live in the `CredentialStore` instance. Access-token
  *validation* is stateless (any instance with the same secret accepts a token),
  but **refresh rotation and logout-revocation do not cross instances** — a token
  revoked on host A still validates on host B until it expires. Run one instance,
  or wait for the durable `SessionStore` seam (not yet built).
- **`role` vs `profile`.** `role` is the coarse string `#[allowed_roles(..)]`
  gate; fine-grained permissions go in `profile`. If you authorize purely by
  profile, pass a constant `role` that satisfies your method annotations (or mark
  those methods `#[public]` and gate them yourself from `caller_profile()`).
- **`caller_profile()` returns `None` for anonymous or role-only callers** —
  always handle the `None` arm; don't `unwrap`.

## Build-template checklist for a new service

1. **`*-contract` crate** — depend on `rpc-macro`, `transport`, `execution`,
   `progressable-future`. Declare your `#[rpc_module]` traits; annotate methods
   `#[public]` / `#[allowed_roles("…")]`. This crate is also the dart/ts-codegen
   source.
2. **`*-api` crate** — depend on `identity`, `transport`, `execution`, `host`.
   Implement your traits; expose a builder that registers the generated responders
   into an `allex_host::ServiceHost` (below). Don't hand-write the `Registry` +
   transport + `serve_dispatch` wiring — `host` owns it.
3. **`*-svc-host` crate** — pick a persistent identity store (or a
   `CredentialStore`), seed with `ensure_user`, build the router, serve.
4. **Dev server** — swap in `InMemoryIdentityStore` + a permissive CORS layer.
5. **Read the caller** inside a method via `allex_serve::caller_handle()` /
   `caller_role()` / `caller_profile()`. Do not add a caller argument.

Stand the modules up with **`allex_host::ServiceHost`** — it owns the `Registry` +
`AxumTransport` + `serve_dispatch` spawn (that spawn order was the one footgun) and
hands back a plain `axum::Router`, so you still add your own CORS / `/health` /
static files / REST routes before serving:

```rust
use allex_host::ServiceHost;

let (router, _dispatch) = ServiceHost::<JsonCodec>::new()
    .register(Arc::new(IdentityV1MethodResponder::<_, JsonCodec>::new(identity.clone())))
    .register(Arc::new(FooV1MethodResponder::<_, JsonCodec>::new(foo_api)))
    .into_router_with_auth(validator)   // any identity store is a BearerTokenValidator
    .into_parts();                      // (Router, JoinHandle); or .with_health().serve(listener)
```

`from_registry(existing)` adopts a `Registry` you built elsewhere. `ServiceHost` is
axum-specific by design; for a non-axum transport, drop to the manual shape — build
the `Registry`, create the transport, call `router*()` to borrow it, then
`tokio::spawn(serve_dispatch(transport, registry))` (that spawn is REQUIRED, since
`serve` itself stays transport-agnostic).

## Running the tests

`cargo test --workspace` — **but** the `allex-postgres` and
`allex-identity-postgres` integration tests require Docker (testcontainers) and
fail with "Docker must be running" otherwise. Without Docker, run:

```
cargo test --workspace --exclude allex-postgres --exclude allex-identity-postgres
```

Lint gate: `cargo clippy --workspace --all-targets -- -D warnings`.
