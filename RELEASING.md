# Releasing allex

allex is a 28-crate workspace published to [crates.io](https://crates.io). The
crates share **one version number** and are released together. Source of truth is
[GitHub](https://github.com/andrija-hers/allex); crates.io receives release
tarballs only.

## Prerequisites (one-time)

- A crates.io account (authenticated via GitHub OAuth) and an API token:
  `cargo login <token>`. Mint the token while logged into the intended GitHub
  account — the publish identity is fixed at token time, not push time.
- Green CI on the commit you intend to release.

## The one hard rule

**A crate can only be published after every crate it depends on is already on
crates.io.** So publishing is **bottom-up** in dependency order. Internal deps are
declared with both `path` and `version`, which is what makes this possible.

## Recommended: `cargo-release` (host-agnostic, no forge API needed)

```sh
cargo install cargo-release
```

`cargo-release` bumps the shared version, updates internal dependency
requirements, creates a git tag, and publishes each crate in the correct order.

Dry run first (does everything except mutate):

```sh
cargo release <level|version>          # e.g. `cargo release minor` or `cargo release 0.2.0`
```

Execute:

```sh
cargo release <level|version> --execute
```

Then push the commit and tag to GitHub:

```sh
git push origin main --follow-tags
```

> Why `cargo-release` and not `release-plz`? `cargo-release` only needs git +
> crates.io. Now that the repository is on GitHub, release-plz's release-PR
> automation would work too; switching is optional.

## Manual fallback (if not using a tool)

Bump `version` in `[workspace.package]` and each internal dependency's `version`,
commit, tag `vX.Y.Z`, then `cargo publish` one crate at a time, **leaves first**.
A workable order (dependencies before dependents):

```
allex-rpc-macros  allex-progressable-future  allex-state-macro  allex-postgres-macros
allex-transport   allex-execution            allex-serve        allex-state
allex-identity    allex-postgres             allex-scheduler    allex-guards
allex-winsvc      allex-script               allex-awaitable-map  allex-watch-singleton
allex-message-templates  allex-mailgunner    allex-keyed-events
allex-identity-sqlite    allex-identity-postgres  allex-identity-redb
allex-dart-codegen       allex-ts-codegen    allex-host
```

`cargo publish` verifies each crate by compiling the packaged tarball, so a green
publish is also a packaging check. Wait for the index to update between a crate
and its dependents (`cargo publish` usually handles this automatically).

## Pre-release checklist

- [ ] CI green (fmt, clippy `-D warnings`, tests, MSRV build).
- [ ] `CHANGELOG.md` `[Unreleased]` section moved under the new version.
- [ ] `cargo publish -p <leaf> --dry-run` passes for the leaf crates.
- [ ] Version bumped in `[workspace.package]` and internal `version` deps.
