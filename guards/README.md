# allex-guards

> Disk/RAM headroom watchdogs — not authorization.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A resource **headroom watchdog** for fire-and-forget services: gauges with
hysteresis that trip an action when disk or RAM gets tight, so you don't
re-implement free-space checks and poll loops per app.

**This is not authorization.** Despite the name, `allex-guards` guards
*resources*, not *rights* — auth lives in `allex-rpc-macros` + `allex-serve` +
`allex-transport` + `allex-identity`.

## Docs & license

API documentation: **[docs.rs/allex-guards](https://docs.rs/allex-guards)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
