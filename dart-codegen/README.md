# allex-dart-codegen

> Generate a typed Dart client from an allex rpc_module source.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Parses the Rust *source* of a `#[rpc_module]` (not macro output), emits only the
DTOs **reachable** from method signatures, and remaps fields to the exact serde
JSON wire keys (`rename_all` honored on struct fields *and* enum variants) — so
the Dart client is byte-compatible with the Rust server.

Pass several sources with repeated `--source` (each parsed independently; DTOs
resolve across files). **The identity client is generated, not hand-written**:
feed `identity/lib.rs` + `identity/types.rs` alongside your own contract sources.

## Example

```sh
allex-dart-codegen \
  --source my-contract/lib.rs \
  --source identity/lib.rs --source identity/types.rs \
  --out lib/api/
```

## Docs & license

API documentation: **[docs.rs/allex-dart-codegen](https://docs.rs/allex-dart-codegen)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
