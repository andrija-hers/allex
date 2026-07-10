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
   error.

## Crate menu

**Contract / codegen**
- `rpc-macro` (`allex-rpc-macros`) — `#[rpc_module]` / `#[allowed_roles]` /
  `#[public]`; generates client, responder, and permission helpers.
- `dart-codegen`, `ts-codegen` — generate Dart / TypeScript clients from a
  `#[rpc_module]` source file. (`serde(rename_all)` is honored on both struct
  fields and enum variants.)

**Runtime**
- `transport` (`allex-transport`) — wire types (`Payload`, `Error`,
  `ModuleMethodCall`), codecs (`JsonCodec`), `AxumTransport`
  (`router` / `router_with_auth`), plus reqwest / signalr / websocket transports;
  defines `BearerTokenValidator`.
- `execution` (`allex-execution`) — `Registry` + `ModuleExecutor` /
  `MethodExecutor` / `StreamingMethodExecutor` dispatch traits.
- `serve` (`allex-serve`) — `serve_dispatch` (drains a transport, spawns a task
  per request) + the `CallerContext` task-local. Deliberately transport-agnostic.
- `progressable-future` (`allex-progressable-future`) — the streaming-RPC future
  and step model.
- `state` / `state-macro` (`allex-state`) — `LocalState` snapshot-delta streams.
- `scheduler` (`allex-scheduler`) — scheduled/recurring tasks.
- `winsvc` (`allex-winsvc`) — Windows service host wrapper.

**Identity** (see the dedicated section below)
- `identity` (`allex-identity`), `identity-sqlite`, `identity-postgres`,
  `identity-redb`.

**Data / util**
- `postgres` / `postgres-macros` (`allex-postgres`) — pooled Postgres access +
  a `FromRow` derive.
- `message-templates` (`allex-message-templates`) — stored message templates
  (tera rendering). `mailgunner` (`allex-mailgunner`) — Mailgun sending.
- `awaitable-map`, `keyed-events`, `watch-singleton`, `script` — async map,
  keyed event bus, single-instance guard, and scripting helpers respectively.

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
2. **`*-api` crate** — depend on `identity`, `transport`, `execution`, `serve`.
   Implement your traits; write a `build_router` that registers the generated
   responders into an `execution::Registry` and wires the transport (pattern
   below).
3. **`*-svc-host` crate** — pick a persistent identity store (or a
   `CredentialStore`), seed with `ensure_user`, call `build_router`, serve.
4. **Dev server** — swap in `InMemoryIdentityStore` + a permissive CORS layer.
5. **Read the caller** inside a method via `allex_serve::caller_handle()` /
   `caller_role()` / `caller_profile()`. Do not add a caller argument.

The transport wiring (kept explicit rather than behind a helper, so `serve`
stays transport-agnostic — forgetting the `serve_dispatch` spawn is the one
footgun, so copy this shape):

```rust
let registry = Arc::new(Registry::new());
registry.register(Arc::new(IdentityV1MethodResponder::<_, JsonCodec>::new(identity.clone())));
registry.register(Arc::new(FooV1MethodResponder::<_, JsonCodec>::new(foo_api)));
// … register each module …

let transport = AxumTransport::<JsonCodec>::new(1024);
let router = transport.router_with_auth(validator);              // borrows transport
tokio::spawn(serve_dispatch::<_, JsonCodec>(transport, registry)); // then moves it — REQUIRED
```

## Running the tests

`cargo test --workspace` — **but** the `allex-postgres` and
`allex-identity-postgres` integration tests require Docker (testcontainers) and
fail with "Docker must be running" otherwise. Without Docker, run:

```
cargo test --workspace --exclude allex-postgres --exclude allex-identity-postgres
```

Lint gate: `cargo clippy --workspace --all-targets -- -D warnings`.
