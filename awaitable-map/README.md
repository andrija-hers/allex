# allex-awaitable-map

> Keyed awaitable fetch-coalescing cache with single-flight dedup.

Part of **[allex](https://codeberg.org/andrija/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

## The problem it solves

You have expensive, keyed lookups — prices, computed reports, remote records —
that many callers want at once. A naïve cache has two holes: when a key is cold, N
concurrent callers each fire their own fetch (a thundering herd), and a fetch that
*fails* gets retried on every subsequent request.

`AwaitableMap` closes both. Ask for keys and await their values; concurrent
requests for a key already in flight **coalesce onto the same fetch**
(single-flight, via per-key `watch` channels). On top of that it adds:

- **pluggable persistence** (`AwaitableMapPersistence`) — bundled SQLite and
  in-memory backends;
- a **failure cooldown** — a failed key isn't re-fetched until the cooldown
  elapses, so a broken upstream isn't hammered;
- **progress reporting** for long fetches (`ProgressableAwaitableMapFetcher`,
  built on `allex-progressable-future`).

`DailyAwaitableMap` partitions the cache by `YYYYMMDD` — one SQLite database per
day — for data that's naturally keyed by date.

## Example

```rust
use allex_awaitable_map::{AwaitableMap, AwaitableMapFetcher};

// Your fetcher does ONE upstream round-trip for all currently-missing keys.
struct PriceFetcher;

impl AwaitableMapFetcher<String, f64> for PriceFetcher {
    type Error = std::io::Error;

    async fn fetch_many(&self, keys: &[String]) -> Result<Vec<(String, f64)>, Self::Error> {
        Ok(keys.iter().map(|k| (k.clone(), look_up_price(k))).collect())
    }
}

// `persistence` is a bundled SQLite or in-memory backend.
let map = AwaitableMap::new(persistence, PriceFetcher);

// Concurrent callers for "AAPL" coalesce into a single fetch_many:
let prices = map.query(&["AAPL".to_string(), "MSFT".to_string()]).await?;
```

## Docs & license

API documentation: **[docs.rs/allex-awaitable-map](https://docs.rs/allex-awaitable-map)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
