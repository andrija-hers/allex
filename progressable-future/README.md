# allex-progressable-future

> A future that emits progress updates before completing.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

The streaming-RPC future underneath every streaming allex method. A
`ProgressableFuture` emits zero or more `Progress(P)` steps followed by exactly
one terminal outcome — `Success(T)`, `Error(E)`, or `Transport(_)`. Consume it as
a stream with `.stream()`, or await only the terminal value with
`.await_result()`.

`no_std`-friendly: the `std` feature (on by default; pulls tokio/futures) can be
swapped for `alloc`.

## Docs & license

API documentation: **[docs.rs/allex-progressable-future](https://docs.rs/allex-progressable-future)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
