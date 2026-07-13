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
use allex_rpc_macros::{rpc_module, public, allowed_roles};

#[rpc_module]
trait CounterV1 {
    #[public]
    fn get(&self) -> impl Future<Output = Result<u64, CounterError>> + Send;

    #[allowed_roles("user")]
    fn increment(&self, by: u64) -> impl Future<Output = Result<u64, CounterError>> + Send;
}
```

## Docs & license

API documentation: **[docs.rs/allex-rpc-macros](https://docs.rs/allex-rpc-macros)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
