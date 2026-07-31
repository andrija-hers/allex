# Changelog

All notable changes to allex are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and allex adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the pre-1.0
caveat that **minor versions may contain breaking changes** until 1.0.

The workspace is versioned and released together: all `allex-*` crates share one
version number.

## [Unreleased]

### Fixed
- **The registry turned "no caller" into a caller.** `Registry`'s dispatch did
  `caller_role.unwrap_or("anonymous")` for its tracing span and then passed that
  string *on* as `Some("anonymous")`, so an unauthenticated request reached the
  permission gate wearing an identity. Harmless for the two older tiers — no role
  list contains `"anonymous"` — but `#[authenticated]` accepts *any* role, so it
  accepted nobody-at-all. Both `ModuleExecutor` and `RelayedModuleExecutor` now
  pass the caller through exactly as it arrived, and the label stays in the span
  where it belongs.

  Worth knowing how it was found: the permission-table unit tests all passed,
  because they call `is_authorized` directly. A live two-node smoke test asking an
  anonymous caller to log out got **200**. The layer that decides was never the
  layer under test.
- **`IdentityV1::logout` was unreachable, and had been for as long as the role
  table has existed.** It carried no access-tier attribute, which the generated
  permission table reads as *nobody may call this* — so every logout over RPC
  answered 403, including from the authenticated callers it was written for, and
  no session was ever actually ended. The refresh token was not deleted either,
  so it stayed valid for its full lifetime after a "sign out".

  Three store test suites covered `logout` and all three passed, because each
  calls the store **directly**: the authorization gate lives in the generated
  responder, which none of them instantiate. A dead endpoint is invisible from
  below. `allex-identity` now asserts the permission table itself.

### Added
- **A legacy-write switch for the payload format**, which turns the wire break
  below into a staged rollout instead of a flag day. `Payload::serialize` consults
  `legacy_payload_writes()`; when on, human-readable formats emit the old array of
  byte values that clients predating this release know how to read. Seeded once
  from `ALLEX_LEGACY_PAYLOAD_WRITES` (`1`/`true`) so a deployment can set it
  without a code change, and overridable at runtime via
  `set_legacy_payload_writes`.

  **Default off**, deliberately: the string form is 3.2–3.4× faster to encode and
  3.1× smaller uncompressed, and a consumer whose only client is served by the
  same host as its API has no compatibility window to buy. Turn it on where
  clients are **installed** — an app behind store review, a sideloaded binary —
  then: deploy with legacy writes on → ship clients that read both → turn it off.

  The sequence matters because readers already accept both forms, so the break is
  only ever in what a peer *writes*. That is also why the switch is enough on its
  own: nothing else has to change on either side.
- **`#[authenticated]` — the third access tier.** "Any caller whose token
  validated, whatever role it carries" was not expressible: there was
  `#[public]` (anyone, anonymous included) and `#[allowed_roles(..)]` (exact
  roles), and nothing in between. That gap is why `logout` shipped with no
  attribute at all — the tier it needed could not be spelled. Generated
  permissions gain `requires_authentication(method)`; `is_role_allowed` accepts
  every role for such a method and still refuses anonymous callers.
- **`IdentityV1::revoke_refresh_token`** (`#[public]`), splitting sign-out along
  the line of what proves you may do it. `logout` (`#[authenticated]`) ends the
  session you are calling with; this ends a refresh token when the session behind
  it can no longer authenticate — an expired access token, a cold-started app, a
  queued sign-out being retried. Possession of the token is the proof, exactly as
  for a password-reset link, and the answer is identical for unknown, expired and
  live tokens so it cannot be used to probe which ones work.
  `CredentialStore` gains the same operation as an inherent method.
- **gzip on unary replies** (`allex-host`), when the client sends
  `Accept-Encoding`. `application/x-ndjson` is deliberately **excluded**: the
  encoder holds a frame until it has enough input to emit a block, so with frames
  produced 300 ms apart the first did not reach the socket until the second
  existed — stale state on a dashboard, traded for bytes that are free on a local
  hop. A `SizeAbove` threshold keeps the encoder off replies too small to gain.
  No client change is needed: browsers and `dart:io` request gzip and decompress
  transparently.

### Changed
- **BREAKING: an `#[rpc_module]` method with no access tier no longer compiles**,
  and neither does one with two. The old behaviour was "no attribute means
  nobody", which is safe and silent — it generates a client method that answers
  403 to every caller. Two attributes silently preferred `#[public]`, resolving an
  ambiguous authorization declaration the most permissive way. The error names the
  three tiers; `#[allowed_roles()]` with an empty list still says "nobody" on
  purpose. Surveyed before landing: across allex, stasera, datascope,
  ISystem-backend and epicfix, **every** real `#[rpc_module]` method already
  declared a tier — the sole exception in the whole neighbourhood was `logout`
  above, so this error can only fire on endpoints that were already dead.
- **BREAKING: `IdentityV1` gained a method**, so hand-written stores must
  implement `revoke_refresh_token`. There is deliberately no default
  implementation: a store that silently accepted a revocation and dropped it would
  repeat the failure this release exists to fix.
- `PostgresIdentityStore::logout` now scopes the refresh-token delete to the
  handle named by the access token, and answers `InvalidRefreshToken` when the
  token belongs to somebody else. A guard against a confused client rather than a
  security boundary — whoever holds a refresh token can already mint access
  tokens with it, which is also why `revoke_refresh_token` needs no session.
- The Dart and TypeScript codegens read the tier **explicitly** instead of
  inferring it from "neither public nor role-restricted". That inference made the
  generated Dart client document `logout` as "Requires authentication." while the
  server refused every caller alive — so the one artifact a consumer actually
  reads described a dead endpoint as an ordinary one, which is why the bug
  survived a year of people looking straight at it. A method with no tier (only
  possible from a source predating this release) is now labelled as refusing
  every caller, and a test in each codegen pins the rendering of all four cases.
  The rule, now in both crates' docs: a generator that cannot tell what the
  source meant must say so, not pick the reading that sounds best.
- **Wire format: payloads travel as JSON strings, not arrays of byte values.**
  `Payload` is still a `Vec<u8>`; only its serialized form changed. Human-readable
  formats now emit the bytes as a **string** when they are valid UTF-8 — which is
  always, for a payload produced by a JSON codec — instead of one JSON number per
  byte. Binary formats and non-UTF-8 payloads keep the byte sequence.

  **This is a breaking change on the wire, not in the Rust API.** `cargo` cannot
  warn you about it: the code compiles, and the mismatch appears only between a
  peer built before this change and one built after.

  - Post-change readers accept **both** forms (the `Payload` `Deserialize` visitor
    handles `visit_str` and `visit_seq`; the Dart client unwraps either). Writers
    emit only the string form.
  - Therefore **both directions break across the boundary**: an old client cannot
    decode a new server's response (its decoder only knows the array form), and an
    old server rejects a new client's request with 400. There is no deploy
    ordering that avoids the gap — see `AGENTS.md`, "Upgrading allex where clients
    are already in the field".
  - Measured, same JSON either way: **3.2–3.4×** faster server-side encode
    (serde_json, release), **3.5–4.2×** faster parse on V8, **2.2–2.5×** on the
    Dart VM, **3.1×** fewer bytes uncompressed (**1.3×** after gzip). The request
    path compounds, because a call's arguments are a payload nested inside the
    envelope payload: a 1 MB inline base64 upload was a **13.00 MB** request body
    and is now **1.33 MB**. Services that sized a max-body limit around the old
    inflation can lower it by an order of magnitude — and uploads that used to
    fail just above 1 MB against a 12 MB limit now succeed.

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
- **0.2.0 also carries the payload wire change above**, which is the more
  consequential of the two for anyone already running allex: the macro break stops
  a build, the wire break stops a deployed client. Version numbers only gate
  consumers that depend on crates.io — a project depending on allex by `path` or
  `git` picks it up on the next `git pull`, with no version bump to notice. Say so
  in the release notes.
- **Bump `dart/allex/pubspec.yaml` in lockstep with 0.2.0.** The Dart package is
  the other half of the wire contract, and the change above did not touch its
  version — both it and the Rust workspace still read `0.1.0`, so a Flutter
  consumer has no way to tell which payload form its copy speaks. The package
  version is the only signal those consumers get.
- **Retiring the legacy-write switch.** It is `Added` in this release (below).
  Once every consumer's installed clients read both forms, the switch and the
  `visit_seq` read path can both go — but not before, and the way to know is
  client-version telemetry rather than a guess.

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
