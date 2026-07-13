# allex-state

> LocalState snapshot-plus-delta streams.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

## The problem it solves

You have live state — a UI grid, a remote mirror, another service — that must
follow changes over a streaming RPC. The tempting approach, **re-sending the whole
collection on every change, is the anti-pattern this crate exists to kill**: it
burns CPU and allocator memory rebuilding — *on the server* — a picture the client
could have derived from a small delta, and it bloats every frame on the wire.

The blessed pattern instead:

- **First frame is the full snapshot** (`StateFrame::Initial`) — a new or
  reconnecting client gets one complete picture to start from.
- **Every later frame is a delta** (`StateFrame::Update`) — only what changed; the
  client applies it to the snapshot it already holds.

`#[derive(State)]` (from `allex-state-macro`) generates, for your struct, an
`Update` enum and **change-suppressing setters**: `set_online(x)` returns
`Some(update)` only if the value actually changed, so an unchanged write is a true
no-op — no lock, no broadcast. `LocalState<S>` is the authoritative in-process
store; `watch_snapshot_deltas` is the *outward* streaming boundary (with
version-dedup, so a reconnecting client that already holds version N skips the
snapshot).

> `subscribe` / `watch_progressable` hand you the *full* value on every change —
> for in-process mirrors and local logic, **not** the outward wire boundary. For
> the wire, use `watch_snapshot_deltas`.

## Example — deriving state and mutating it

```rust
use allex_state::{State, LocalState};

#[derive(State, Clone, Debug, PartialEq)]
struct Dashboard {
    #[state] online: u32,
    #[state] title: String,
    ignored_cache: bool,        // no #[state] -> not part of the Update model
}

let state = LocalState::new(Dashboard {
    online: 0,
    title: "Live".into(),
    ignored_cache: false,
});

// generated, change-suppressing setter; broadcasts only on a real change
state.mutate(|d| d.set_online(1));
state.mutate(|d| d.set_online(1));   // same value -> suppressed, no broadcast
```

## Streaming snapshot + deltas to clients

`watch_snapshot_deltas(known_version, version_of, diff, on_closed)` returns a
`ProgressableFuture<StateFrame<S, C>>` — the initial snapshot followed by diff
frames — which a streaming `#[rpc_module]` method can return directly. The
closures (how to *version* a state, how to *diff* two states into your change type
`C`) are yours, so the mechanics are best seen worked end-to-end: see
[`examples/snapshot_deltas.rs`](examples/snapshot_deltas.rs) and the full guide in
`SNAPSHOT-DELTAS.md`.

## Docs & license

API documentation: **[docs.rs/allex-state](https://docs.rs/allex-state)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
