# allex-rpc-macros

> The proc-macro that makes a service *a trait*.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

`#[rpc_module] trait FooV1 { … }` expands into a typed `FooV1Client`, a
server-side `FooV1MethodResponder`, and `FooV1MethodPermissions`. A method's
**return type** — not an attribute — decides unary vs. streaming: return
`impl Future<Output = Result<T, E>> + Send` for unary, a `ProgressableFuture`
(from `allex-progressable-future`) for streaming.

Authorization is declarative and compiled in: annotate methods `#[public]`
(anyone, including anonymous) or `#[allowed_roles("admin", "user")]`. The
generated responder checks the caller's role **before** decoding the request and
returns `AccessForbidden` (HTTP 403) on failure — you never hand-write a role
check.

## Example

```rust
use allex_rpc_macros::rpc_module; // #[public] / #[allowed_roles] are inner attributes of #[rpc_module]

#[derive(serde::Serialize, serde::Deserialize)]
pub enum CounterError { Overflow }

#[rpc_module]
pub trait CounterV1 {
    #[public]
    fn get(&self) -> impl Future<Output = Result<u64, CounterError>> + Send;

    #[allowed_roles("user")]
    fn increment(&self, by: u64) -> impl Future<Output = Result<u64, CounterError>> + Send;
}
```

## Required dependencies

The generated code references the contract crates **by bare name**, so a crate that
uses `#[rpc_module]` must depend on them under exactly these names:

```toml
[dependencies]
allex-rpc-macros = "0.1"
transport = { package = "allex-transport", version = "0.1" }
execution = { package = "allex-execution", version = "0.1" }
progressable-future = { package = "allex-progressable-future", version = "0.1" }
async-trait = "0.1"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
```

Types crossing the wire (requests, responses, errors) must be `pub` and implement
`Serialize` / `Deserialize`. (Removing the need for these aliases — making the
macro emit self-contained paths — is planned for **0.2.0**.)

## Docs & license

API documentation: **[docs.rs/allex-rpc-macros](https://docs.rs/allex-rpc-macros)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
