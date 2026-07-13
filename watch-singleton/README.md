# allex-watch-singleton

> A 'static, hot-reloadable config singleton.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

A process-wide configuration singleton with a **push-on-change** pattern: register
an `Apply` handler, and when the config is reloaded, changes are pushed to
registered consumers rather than polled. Handy for config a few subsystems must
react to live.

## Docs & license

API documentation: **[docs.rs/allex-watch-singleton](https://docs.rs/allex-watch-singleton)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
