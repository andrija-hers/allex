# Changelog

All notable changes to allex are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and allex adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0
caveat that **minor versions may contain breaking changes** until 1.0.

The workspace is versioned and released together: all `allex-*` crates share one
version number.

## [Unreleased]

First public release preparation. Nothing is on crates.io yet.

### Added
- Dual `MIT OR Apache-2.0` license (`LICENSE-MIT`, `LICENSE-APACHE`).
- Complete package metadata across all crates (description, keywords, repository,
  homepage, authors, license) via `[workspace.package]` inheritance.
- `README.md` (project front door) and this changelog.
- `SECURITY.md` with a private reporting channel and the identity security model.
- Woodpecker CI: rustfmt, clippy (`-D warnings`), and tests, pinned to the MSRV toolchain.
- `docs.rs` metadata for the Windows-only and feature-gated crates.

### Fixed
- **Calling an unregistered module now returns HTTP 404**, not 500. The axum
  transport maps `transport::Error::ModuleNotFound` to 404, and the reqwest client
  maps a 404 response back to `ModuleNotFound`. (An unknown *method* on a
  registered module is caught earlier by the authorization gate and returns 403;
  whether that should instead be 404 is an open design question.)

### Notes
- **MSRV:** see `rust-version` in `Cargo.toml`. It is verified in CI.
- Example crates under `examples/` are marked `publish = false`.
