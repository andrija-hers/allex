# allex

**A batteries-included, contract-first RPC framework for Rust.** You declare a
service as a trait; a proc-macro generates the client, the server glue, and the
authorization checks — and separate codegen emits byte-compatible **Dart** and
**TypeScript** clients from the same source. Runtime crates handle transport,
dispatch, identity, streaming, and state.

This is the **umbrella crate**: it re-exports the individual `allex-*` crates
under short module names, feature-gated, so you can pull in the framework with one
dependency and enable only what you use.

> Status: **0.x, pre-1.0.** The API will change between minor versions. Developed
> on [Codeberg](https://codeberg.org/andrija/allex); the full README, the
> orientation guide `AGENTS.md`, and `COMPARISON.md` live there.

## Install

```toml
[dependencies]
allex = "0.1"     # default features: ["host", "identity"] — "stand up a backend"
```

Pick features explicitly for a leaner build:

```toml
allex = { version = "0.1", default-features = false, features = ["contract", "codegen"] }
```

## Features → crates

| Feature | Pulls in | Gives you |
|---|---|---|
| `host` *(default)* | `serve` + `contract` + `allex-host` | the axum `ServiceHost` and the whole serve/contract stack |
| `identity` *(default)* | `allex-identity` | `IdentityV1`, the JWT engine, credential/account seams |
| `contract` | rpc-macros, transport, execution, progressable-future | declare and dispatch a `#[rpc_module]` |
| `serve` | `contract` + `allex-serve` | the dispatch loop + ambient caller context |
| `identity-sqlite` / `-postgres` / `-redb` | the store | a ready-made identity store |
| `state` · `scheduler` · `keyed-events` | the crate | live state streams · tick engine · NOTIFY feeds |
| `codegen` | dart + ts codegen | generate typed clients from your contract |
| `postgres` · `awaitable-map` · `message-templates` · `mailgunner` · `watch-singleton` · `guards` · `script` | the crate | the matching utility |
| `full` | everything | all of the above |

Each maps to a module: `allex::host`, `allex::identity`, `allex::transport`,
`allex::progressable`, `allex::state`, … plus the `allex::rpc_module` /
`allex::public` / `allex::allowed_roles` macros at the crate root.

## A taste

Declare the contract:

```rust
use allex::rpc_module; // #[public] / #[allowed_roles] are inner attributes of #[rpc_module]

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
use allex::host::ServiceHost;
use allex::transport::JsonCodec;

let (router, _dispatch) = ServiceHost::<JsonCodec>::new()
    .register(Arc::new(IdentityV1MethodResponder::<_, JsonCodec>::new(identity)))
    .register(Arc::new(CounterV1MethodResponder::<_, JsonCodec>::new(counter)))
    .into_router_with_auth(validator)   // any identity store is a validator
    .into_parts();                      // -> (axum::Router, JoinHandle)
```

Read the caller inside a method — it's an ambient task-local, not an argument:

```rust
let who = allex::serve::caller_handle()?;   // also caller_role(), caller_profile()
```

The full walkthrough — the contract → api → svc-host crate layout, the identity
decision guide, the error→HTTP-status rules, and how allex compares to gRPC / tRPC
/ ConnectRPC — is in the [repository](https://codeberg.org/andrija/allex).

## Prefer the individual crates?

You don't have to use this umbrella. Every piece is published on its own
(`allex-host`, `allex-transport`, `allex-identity`, …) and can be depended on
directly; the utility crates (`allex-scheduler`, `allex-awaitable-map`,
`allex-watch-singleton`, `allex-script`, …) are usable with no RPC core at all.

## License

Dual-licensed under **MIT OR Apache-2.0**, at your option.
