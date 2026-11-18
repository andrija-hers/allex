# allex-execution

> The routing seam: registry + dispatch traits.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

## The problem it solves

allex splits an RPC server into two worlds: `allex-transport` speaks **bytes and
wire-visible names** (`foo_v_1` + method), while a generated
`<Trait>MethodResponder` speaks **typed handlers**. Something has to route a
decoded call from the first world to the right handler in the second — and decide,
at that boundary, whether a returned error is a *domain* error or an
*authorization/transport* fault. That "something" is this crate.

You rarely call it directly: `#[rpc_module]` implements the dispatch traits and
`allex-serve` drives them. **Your one touchpoint is building a `Registry` and
`register`-ing responders into it** — and even that is usually done for you by
`allex-host`.

## What a `Registry` is

A `DashMap`-backed router with three tables — unary modules, streaming modules,
and relayed upstream servers — each keyed by the `foo_v_1`-style wire module name.
A generated responder implements *both* `MethodExecutor` and
`StreamingMethodExecutor`, so a single `register` call wires its unary **and**
streaming methods at once. `Registry` itself implements `ModuleExecutor`, so the
whole server routes as one object.

## Where HTTP 422 vs 403 is born

Generated responders return a rich `Response` (`Success` / `Progress` / `Error` /
`TransportError`). The blanket `MethodExecutor` impl flattens it:

- `Error(payload)` → `Err(Error::App(_))` — a **domain** error → HTTP **422**
- `TransportError(err)` → `Err(err)`, passed through unwrapped, so an auth
  rejection (`AccessForbidden`) stays a transport fault → HTTP **403**

That single flattening is why domain failures and authorization failures get
different status codes without you writing any mapping.

Addressing errors are a third category. Calling an unregistered **module** yields
`Error::ModuleNotFound` (from `Registry`) → HTTP **404** — a client mistake, not a
server fault. (A call to a non-existent **method** on a *registered* module is
caught earlier, by the responder's authorization gate, and currently returns
**403**.)

## Example

```rust
use allex_execution::Registry;
use std::sync::Arc;

let registry = Registry::new();

// A generated responder is both MethodExecutor + StreamingMethodExecutor,
// so one call wires its unary AND streaming methods:
registry.register(Arc::new(
    CounterV1MethodResponder::<_, JsonCodec>::new(counter),
));

// `Registry` is itself a `ModuleExecutor`; hand it to `allex_serve::serve_dispatch`
// — or let `allex-host` own this wiring for you.
```

## Docs & license

API documentation: **[docs.rs/allex-execution](https://docs.rs/allex-execution)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
