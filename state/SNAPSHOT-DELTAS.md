# The snapshot-deltas pattern

**The one blessed way to communicate live State to the outer world** (a UI grid, a remote mirror,
another service) in allex. If you are wiring a streaming RPC that pushes a changing collection to
clients, this is the pattern. Use it.

## The rule

- **The first frame a client receives is a full snapshot** — one complete picture to start from.
- **Every frame after that is a delta** — *only what changed* since the previous frame. The client
  keeps the snapshot it already holds and applies each delta to it.

That's it. Snapshot once, then changes forever.

## The anti-pattern (do NOT do this)

**A full snapshot per frame is not the way to communicate.** Re-sending the whole collection on
every change is the mistake this pattern exists to prevent:

- It burns CPU and allocator memory rebuilding — *on the server* — state the client could derive
  from a small delta. Under load this shows up as steady RSS growth from allocation churn.
- It bloats every frame with data that didn't change.
- It rebuilds something the client can produce itself. **The server should do its work and tell the
  outer world only _what changed_.** Nothing a client can reconstruct should be rebuilt server-side.

> Caveat: this rule is about the **outward wire boundary**. The *in-process* `LocalState` holding the
> full value in a `watch` channel is fine and expected — `subscribe()` / `watch_progressable()` yield
> the full value and are for in-process mirrors and local logic. Just don't pipe a full snapshot per
> change to *clients*.

If you ever want a full, human-readable view (e.g. for `curl` debugging), expose a separate
*request/response* read endpoint that returns the snapshot on demand. Do **not** make the streaming
path send full snapshots for debuggability — it isn't worth the cost.

## The API

`allex-state` gives you the pattern directly:

```rust
use allex_state::{LocalState, StateFrame};

// Your streaming RPC handler:
state.watch_snapshot_deltas(
    req.known_version,        // Option<u64>: client echoes the version it already has (reconnect)
    |s| s.version,            // version_of: pull the monotonic version from a snapshot
    |prev, cur| diff(prev, cur), // diff: the net changes between two snapshots (YOUR code)
    || StateClosedError,      // on_closed: error to emit if the owning task drops the state
)
// -> ProgressableFuture<StateFrame<S, C>, _, E>
```

It emits:

```rust
enum StateFrame<S, C> {
    Initial { version: u64, snapshot: S },  // first frame (or a forced resync)
    Update  { version: u64, changes: Vec<C> }, // every frame after
}
```

- The **initial snapshot is skipped** when `known_version` matches the current version — a
  reconnecting client that already has version *N* resumes straight into deltas.
- **Empty diffs are suppressed** — no idle frames when nothing semantically changed.
- The underlying `watch` **coalesces**, so a slow client that misses intermediate states still gets
  a correct *net* delta (the diff is computed against the last state the stream actually sent). No
  lag-resync bookkeeping required.

### The one piece you write: the diff

`allex-state` cannot diff your state for you, because only you know **domain identity** — which field
is the key that says "this is the same row, updated" vs. "this is a new row." So you provide:

1. **A change type `C`** — almost always `enum { Upsert(Item), Remove(Key) }`.
2. **A `diff(prev, cur) -> Vec<C>`** — keyed by your identity: upsert new/changed items, remove
   absent ones.

```rust
fn diff(prev: &Grid, cur: &Grid) -> Vec<RowChange> {
    let prev_by: HashMap<&str, &Row> = prev.rows.iter().map(|r| (r.id.as_str(), r)).collect();
    let cur_ids: HashSet<&str> = cur.rows.iter().map(|r| r.id.as_str()).collect();
    let mut changes = Vec::new();
    for r in &cur.rows {                                   // new or changed → upsert
        if prev_by.get(r.id.as_str()).map(|p| **p == *r) != Some(true) {
            changes.push(RowChange::Upsert(r.clone()));
        }
    }
    for r in &prev.rows {                                  // gone → remove
        if !cur_ids.contains(r.id.as_str()) {
            changes.push(RowChange::Remove(r.id.clone()));
        }
    }
    changes
}
```

## What the client does

1. On `Initial { snapshot }` → hold it.
2. On `Update { changes }` → apply each change (upsert by key, remove by key) to the held snapshot.
3. On reconnect → send the last `version` it successfully applied as `known_version`, so the server
   skips a redundant snapshot.

## Working code

- Runnable: [`examples/snapshot_deltas.rs`](examples/snapshot_deltas.rs) —
  `cargo run -p allex-state --example snapshot_deltas`.
- Tests pinning the contract: [`tests/snapshot_deltas.rs`](tests/snapshot_deltas.rs) — in particular
  *"applying the deltas to the initial snapshot reconstructs the live state exactly."*
- Real-world reference: datascope's `ActiveSchedulesV1::watch_active_schedules`
  (`ActiveSchedulesStreamProgress { Initial | Update }`) — the implementation this primitive was
  generalized from.

## Checklist when adding a new streamed collection

- [ ] Authoritative state in a `LocalState<S>` with a monotonic `version` bumped on every change.
- [ ] A wire change type `C` (`Upsert`/`Remove` by identity) and a `diff(prev, cur) -> Vec<C>`.
- [ ] Streaming RPC returns `watch_snapshot_deltas(known_version, version_of, diff, on_closed)`.
- [ ] Request carries `known_version: Option<u64>` for reconnect dedup.
- [ ] Client applies `Initial` then `Update` deltas; echoes `version` on reconnect.
- [ ] You did **not** rebuild or re-send a full snapshot on the streaming path.
