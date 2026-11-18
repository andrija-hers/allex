# allex-scheduler

> A pure, clock-free tick engine for recurring tasks.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A scheduler that is deliberately *not* a timer. It owns no clock and spawns no
threads: you pass it "now", and it tells you what should have fired since the last
tick. That makes scheduling logic trivially testable and **DST-correct** — it
reasons in real instants, not naïve wall-clock arithmetic.

Drive it from whatever loop or timer you already have.

## Docs & license

API documentation: **[docs.rs/allex-scheduler](https://docs.rs/allex-scheduler)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
