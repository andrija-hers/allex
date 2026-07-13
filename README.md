# allex

**A batteries-included, contract-first RPC framework for Rust.** You declare a
service as a trait; a proc-macro generates the client, the server glue, and the
authorization checks — and separate codegen emits byte-compatible **Dart** and
**TypeScript** clients from the same source. Runtime crates handle transport,
dispatch, identity, streaming, and state.

> Status: **0.x, pre-1.0.** The API will change between minor versions. allex is
> developed on [Codeberg](https://codeberg.org/andrija/allex); releases are
> published to crates.io. See [Support & contributions](#support--contributions).

## Why allex

- **One trait is the whole contract.** `#[rpc_module] trait FooV1 { … }` expands
  into a typed client, a server responder, and permission helpers. The method
  *return type* — not an attribute — decides unary vs. streaming.
- **Authorization is declarative and enforced for you.** `#[public]` or
  `#[allowed_roles("admin")]` on the trait; the generated responder checks the
  caller's role *before* decoding the request. You never hand-write a role check.
- **The clients aren't hand-written.** `allex-dart-codegen` / `allex-ts-codegen`
  parse the Rust source and emit typed clients that match the exact serde JSON
  wire format, so every language stays in sync with the contract.
- **A host with the footguns already handled.** `allex-host::ServiceHost` owns the
  registry + transport + dispatch wiring and hands you back a plain `axum::Router`.
- **An identity system in the box.** `IdentityV1` (register/login/refresh/logout)
  with ready-made SQLite / Postgres / redb / in-memory stores, a JWT engine, and
  seams for custom credential backends and per-user rights.

## A taste

Declare the contract:

```rust
use allex_rpc_macros::{rpc_module, public, allowed_roles};

#[rpc_module]
trait CounterV1 {
    #[public]
    fn get(&self) -> impl Future<Output = Result<u64, CounterError>> + Send;

    #[allowed_roles("user")]
    fn increment(&self, by: u64) -> impl Future<Output = Result<u64, CounterError>> + Send;
}
```

Stand it up as an axum service:

```rust
use allex_host::ServiceHost;
use allex_transport::JsonCodec;

let (router, _dispatch) = ServiceHost::<JsonCodec>::new()
    .register(Arc::new(IdentityV1MethodResponder::<_, JsonCodec>::new(identity)))
    .register(Arc::new(CounterV1MethodResponder::<_, JsonCodec>::new(counter)))
    .into_router_with_auth(validator)   // any identity store is a validator
    .into_parts();                      // -> (axum::Router, JoinHandle)
```

Read the caller inside a method — it's an ambient task-local, not an argument:

```rust
let who = allex_serve::caller_handle()?;   // also caller_role(), caller_profile()
```

Full walkthrough — including the contract → api → svc-host crate layout, the
identity decision guide, and the error→HTTP-status rules — is in
**[AGENTS.md](AGENTS.md)**, the project's orientation doc. Runnable code lives in
[`examples/`](examples/).

## How allex compares

allex overlaps with **gRPC**, **tRPC**, and **ConnectRPC** on one idea —
*contract-first RPC where you don't hand-write the client* — but occupies a niche
none of them do: **a Rust-native, contract-from-code (a Rust trait, no IDL) RPC
*framework* with generated Flutter/Dart and browser/TS clients, and identity,
authorization, and hosting in the box.** Each peer beats allex on its own axis
(gRPC on performance and polyglot reach, tRPC inside a pure-TypeScript stack,
ConnectRPC on schema governance and gRPC interop), and allex is honest about its
gaps (JSON-only wire today, server-push-only streaming, 0.x and solo-maintained).

**[COMPARISON.md](COMPARISON.md)** has the full side-by-side table, the one-line
take on each competitor, and the honest-gaps and performance notes.

## The crates

Everything is published as `allex-*`. A brief map (see each crate's `//!` docs and
[AGENTS.md](AGENTS.md) for detail):

**Contract & codegen** — `allex-rpc-macros`, `allex-dart-codegen`, `allex-ts-codegen`
**Runtime** — `allex-transport`, `allex-execution`, `allex-serve`, `allex-host`,
`allex-progressable-future`, `allex-state` / `allex-state-macro`, `allex-scheduler`,
`allex-keyed-events`, `allex-winsvc`
**Identity** — `allex-identity` + stores `allex-identity-sqlite` / `-postgres` / `-redb`
**Data & utilities** — `allex-postgres` / `allex-postgres-macros`, `allex-mailgunner`,
`allex-message-templates`, `allex-awaitable-map`, `allex-watch-singleton`,
`allex-guards`, `allex-script`

The utility crates (`allex-awaitable-map`, `allex-scheduler`, `allex-watch-singleton`,
`allex-script`, `allex-mailgunner`, …) are largely standalone and usable without the
RPC core.

## Installing

Once the first release is on crates.io:

```toml
[dependencies]
allex-host = "0.1"
```

Until then (or to track development), depend on it from Codeberg:

```toml
[dependencies]
allex-host = { git = "https://codeberg.org/andrija/allex", tag = "v0.1.0" }
```

**MSRV:** Rust 1.97. allex tracks a recent stable toolchain (its dependencies and
some trait-solver-dependent code require it) and does not yet commit to an older
floor. Newer toolchains are fine.

## Building & testing

```sh
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The `allex-postgres` and `allex-identity-postgres` integration tests need Docker
(testcontainers). Without it, exclude them:

```sh
cargo test --workspace --exclude allex-postgres --exclude allex-identity-postgres
```

## Support & contributions

allex is built in the open, but it is **solo-maintained and offered as-is** (see
the license). Concretely, that means:

- **No support guarantee and no SLA.** Bug reports and questions are welcome via
  the [Codeberg issue tracker](https://codeberg.org/andrija/allex/issues), but
  there is no promise of a response time, a fix, or long-term support.
- **The roadmap is the maintainer's.** Feature requests are read with interest,
  but allex is shaped by its own goals; "won't do" is a valid and common answer,
  and it isn't personal.
- **Patches:** small, focused pull requests with a clear rationale are the most
  likely to land. For anything larger, open an issue to discuss direction first,
  so you don't invest in a change that doesn't fit. By submitting a contribution
  you agree to license it under the same terms as allex (below).

If allex is useful to you, that's great — please build on it. Just calibrate your
expectations to a one-person project, not a vendor.

## Authors

allex is built by **Andrija Petrović** and **Luka Petrović**.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution you
intentionally submit for inclusion in allex, as defined in the Apache-2.0 license,
shall be dual-licensed as above, without any additional terms or conditions.
