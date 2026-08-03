# Contributing to allex

Thanks for your interest. Please read this first — it's short, and it sets honest
expectations.

## What allex is (for contributors)

allex is **solo-maintained and offered as-is**. Contributions are welcome, but:

- The roadmap is the maintainer's. "Doesn't fit / won't merge" is a normal answer
  and isn't personal.
- There is no response-time or review-time guarantee.
- For anything more than a small fix, **open an issue to discuss direction
  first**, so you don't invest in a change that won't land.

## Before you open a pull request

Run every gate with one command:

```sh
scripts/ci.sh           # everything
scripts/ci.sh --rust    # Rust only
scripts/ci.sh --dart    # the Dart package only
```

That covers `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
-- -D warnings`, the workspace tests, the Postgres integration tests (only when
a usable Docker is present — they need testcontainers), and `dart analyze` plus
`dart test` for `dart/allex`. Anything it could not run is reported as a `SKIP`
rather than folded into a pass, so a green summary means what it says.

The equivalent by hand:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude allex-postgres --exclude allex-identity-postgres
```

(The two excluded crates have integration tests that need Docker /
testcontainers; run the full `cargo test --workspace` with Docker up if your
change touches Postgres.)

Keep pull requests **small and focused**, with a clear rationale in the
description. Match the style of the surrounding code. Update `CHANGELOG.md` under
`[Unreleased]` if your change is user-visible.

## Orientation

[AGENTS.md](AGENTS.md) is the map of the workspace — the load-bearing concepts,
the crate menu, and the "build a new service" checklist. Each crate's `//!`
module docs carry the detail. Start there before diving into a specific crate.

## Licensing of contributions

By submitting a contribution you agree to license it under the same terms as
allex — dual **MIT OR Apache-2.0** — as stated in the Apache-2.0 license text.
No CLA, no copyright assignment.

## Security

Do **not** report security vulnerabilities in public issues. See
[SECURITY.md](SECURITY.md) for the private reporting channel.
