# Changelog

All notable changes to allex are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and allex adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0
caveat that **minor versions may contain breaking changes** until 1.0.

The workspace is versioned and released together: all `allex-*` crates share one
version number.

## [Unreleased]

### Documentation
- Corrected the `#[rpc_module]` usage examples (`README.md`, the `allex` and
  `allex-rpc-macros` READMEs, `AGENTS.md`). A contract crate must depend on the
  contract crates aliased to the bare names the macro emits — `transport`
  (`allex-transport`), `execution` (`allex-execution`), `progressable-future`
  (`allex-progressable-future`) — plus `async-trait`, `futures`, and `serde`; and
  wire types must be `pub`. The earlier examples (`cargo add allex` → `#[rpc_module]`)
  did not compile as shown. Verified via a from-crates.io smoke test.

### Planned
- **0.2.0 — macro path-hygiene.** `#[rpc_module]` emits bare crate paths
  (`::transport`, `::execution`, …), so it does not compile from a single `allex`
  dependency without the aliased contract deps above. Making the macro
  self-contained is planned for 0.2.0; it is a breaking change for consumers that
  rely on the current bare-name aliases.

## [0.1.0] - 2026-07-13

First public release — all 25 `allex-*` crates plus the `allex` umbrella,
published to crates.io.

### Added
- Dual `MIT OR Apache-2.0` license (`LICENSE-MIT`, `LICENSE-APACHE`).
- Complete package metadata across all crates (description, keywords, repository,
  homepage, authors, license) via `[workspace.package]` inheritance.
- `allex` umbrella crate re-exporting the framework under feature flags
  (default = `host` + `identity`), so `cargo add allex` pulls the runtime stack.
- `README.md`, per-crate READMEs, `COMPARISON.md`, `CONTRIBUTING.md`,
  `RELEASING.md`, and this changelog.
- `SECURITY.md` with a private reporting channel and the identity security model.
- Woodpecker CI: rustfmt, clippy (`-D warnings`), and tests, pinned to the MSRV toolchain.
- `docs.rs` metadata for the Windows-only and feature-gated crates.

### Fixed
- **Calling an unregistered module now returns HTTP 404**, not 500. The axum
  transport maps `transport::Error::ModuleNotFound` to 404, and the reqwest client
  maps a 404 response back to `ModuleNotFound`. (An unknown *method* on a
  registered module is caught earlier by the authorization gate and returns 403 —
  a deliberate choice, documented in the status table.)

### Notes
- **MSRV:** Rust 1.97 (see `rust-version` in `Cargo.toml`); verified in CI.
- Example crates under `examples/` are marked `publish = false`.
