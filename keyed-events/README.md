# allex-keyed-events

> Cross-instance live event feeds over Postgres NOTIFY.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

The three pieces of a cross-instance live feed:

- **`KeyedBroadcastHub`** — per-key, in-process fan-out.
- **`spawn_pg_notify_listener`** — a reconnecting Postgres `LISTEN` driver.
- **`replay_and_watch`** — the streaming-RPC glue: subscribe, replay a backlog,
  then forward live events deduplicated by a monotonic id, returning the
  `ProgressableFuture` your method returns.

Building a `watch_*` streaming method over a NOTIFY bus? Call `replay_and_watch`
instead of re-writing the subscribe/backlog/dedup loop. Fold per-request
authorization into the backlog closure — its `Err` becomes the terminal frame
before any live event is seen.

## Docs & license

API documentation: **[docs.rs/allex-keyed-events](https://docs.rs/allex-keyed-events)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
