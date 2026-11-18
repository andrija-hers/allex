# allex-ts-codegen

> Generate a typed TypeScript client from an allex rpc_module source.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

Like its Dart sibling, it parses Rust source, emits only the DTOs reachable from
method signatures, and matches the exact serde JSON wire format (`rename_all` on
fields and enum variants) so the client stays in lockstep with the server.

Pass multiple `--source` files (each parsed independently); the guarded golden
output lives in `tests/golden/`.

## Example

```sh
allex-ts-codegen \
  --source my-contract/lib.rs \
  --source identity/lib.rs --source identity/types.rs \
  --out src/api/
```

## Docs & license

API documentation: **[docs.rs/allex-ts-codegen](https://docs.rs/allex-ts-codegen)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
