# allex-script

> Run an external script/executable with a typed result.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Run an external script or executable as a typed operation: pass arguments and
environment in, get captured output out, and a **non-zero exit code mapped to a
typed error**. Cross-platform and shell-agnostic — it invokes the executable
directly rather than through a shell, so there's no quoting/injection surprise.

## Docs & license

API documentation: **[docs.rs/allex-script](https://docs.rs/allex-script)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
