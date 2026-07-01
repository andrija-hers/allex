//! Cross-instance, per-key live event fan-out.
//!
//! Two composable primitives, extracted from a chat broadcast hub once a second
//! consumer (a per-user notification feed) needed the same shape:
//!
//! - [`KeyedBroadcastHub`] — a sharded, self-GC'ing in-process broadcaster keyed
//!   by `K` (an activity id, a user handle, …). Subscribers get a
//!   [`tokio::sync::broadcast`] receiver; the entry is dropped once its last
//!   receiver goes away, so quiet keys don't leak.
//! - [`spawn_pg_notify_listener`] — drives a dedicated, reconnecting Postgres
//!   `LISTEN` connection and hands each `(channel, payload)` to a callback.
//!
//! Composed, they give "what just happened on key K, across all instances":
//! emit `pg_notify('chan', json_with_a_key)` wherever the event is written; one
//! listener per instance re-broadcasts received payloads into the local hub;
//! per-key watchers (e.g. a streaming RPC) subscribe and forward live.
//!
//! Note: `LISTEN` needs a real Postgres *session*, so the connection string must
//! NOT go through a transaction-pooling proxy (PgBouncer in transaction mode).
//! Point it straight at Postgres.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::broadcast;

/// A sharded, self-GC'ing in-process broadcaster keyed by `K`.
///
/// Entries are created lazily on first [`subscribe`](Self::subscribe) and
/// garbage-collected once their last receiver is gone — either via a
/// [`Subscription`] guard on drop, or by a [`publish`](Self::publish) that finds
/// no receivers. Sharding spreads lock contention across keys.
pub struct KeyedBroadcastHub<K, E> {
    shards: Vec<Mutex<HashMap<K, broadcast::Sender<E>>>>,
    capacity: usize,
}

impl<K, E> KeyedBroadcastHub<K, E>
where
    K: Eq + Hash + Clone,
    E: Clone + Send + 'static,
{
    /// `shard_count` independently-locked maps; each broadcaster buffers
    /// `capacity` events (slow receivers beyond that see `Lagged`).
    pub fn new(shard_count: usize, capacity: usize) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            capacity: capacity.max(1),
        }
    }

    fn shard(&self, key: &K) -> &Mutex<HashMap<K, broadcast::Sender<E>>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % self.shards.len()]
    }

    /// A live receiver for `key`, creating the broadcaster if needed. The new
    /// receiver is registered while the shard is locked, so it can't race with a
    /// concurrent GC of the same entry. Prefer [`subscribe_guarded`] so the entry
    /// is reaped automatically when the receiver is dropped.
    ///
    /// [`subscribe_guarded`]: Self::subscribe_guarded
    pub fn subscribe(&self, key: &K) -> broadcast::Receiver<E> {
        let mut map = self.shard(key).lock().unwrap();
        map.entry(key.clone())
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .subscribe()
    }

    /// Delivers `event` to any live subscribers of `key`. If the broadcaster has
    /// no receivers left, the send fails and the dead entry is reaped in place.
    pub fn publish(&self, key: &K, event: E) {
        let mut map = self.shard(key).lock().unwrap();
        if let Some(tx) = map.get(key)
            && tx.send(event).is_err()
        {
            map.remove(key);
        }
    }

    /// Drops the broadcaster for `key` if no receivers remain. The check runs
    /// under the shard lock, so a subscribe that just incremented the count (and
    /// holds the same lock) can't have its fresh entry reaped.
    pub fn release(&self, key: &K) {
        let mut map = self.shard(key).lock().unwrap();
        if let Some(tx) = map.get(key)
            && tx.receiver_count() == 0
        {
            map.remove(key);
        }
    }

    /// Subscribe and get an RAII [`Subscription`] that GCs the entry on drop
    /// (including on task cancellation).
    pub fn subscribe_guarded(self: &Arc<Self>, key: &K) -> Subscription<K, E> {
        let rx = self.subscribe(key);
        Subscription {
            hub: Arc::clone(self),
            key: key.clone(),
            rx: Some(rx),
        }
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().len()).sum()
    }

    #[cfg(test)]
    fn occupied_shards(&self) -> usize {
        self.shards
            .iter()
            .filter(|s| !s.lock().unwrap().is_empty())
            .count()
    }
}

impl<K, E> Default for KeyedBroadcastHub<K, E>
where
    K: Eq + Hash + Clone,
    E: Clone + Send + 'static,
{
    /// 16 shards, 256-event buffer.
    fn default() -> Self {
        Self::new(16, 256)
    }
}

/// A held subscription that releases its hub entry when dropped — drop the
/// receiver first (it's reset before `release`) so the receiver-count check
/// excludes this subscriber.
pub struct Subscription<K: Eq + Hash + Clone, E: Clone + Send + 'static> {
    hub: Arc<KeyedBroadcastHub<K, E>>,
    key: K,
    rx: Option<broadcast::Receiver<E>>,
}

impl<K: Eq + Hash + Clone, E: Clone + Send + 'static> Subscription<K, E> {
    /// Receive the next event. Mirrors [`broadcast::Receiver::recv`] (a slow
    /// receiver may see [`broadcast::error::RecvError::Lagged`]).
    pub async fn recv(&mut self) -> Result<E, broadcast::error::RecvError> {
        self.rx.as_mut().expect("subscription is live").recv().await
    }
}

impl<K: Eq + Hash + Clone, E: Clone + Send + 'static> Drop for Subscription<K, E> {
    fn drop(&mut self) {
        self.rx = None; // drop the receiver first so release() excludes it
        self.hub.release(&self.key);
    }
}

/// Spawns a background task that keeps a dedicated Postgres `LISTEN` connection
/// open on `channels` and calls `handler(channel, payload)` for every
/// notification, reconnecting on loss. `label` only tags reconnect logs.
///
/// The returned [`JoinHandle`](tokio::task::JoinHandle) drives the reconnect
/// loop; dropping it does not stop listening (the task is detached) — abort it
/// to stop.
pub fn spawn_pg_notify_listener<F>(
    label: impl Into<String>,
    conn_string: String,
    channels: Vec<String>,
    handler: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(&str, &str) + Send + Sync + 'static,
{
    let label = label.into();
    let handler = Arc::new(handler);
    tokio::spawn(async move {
        loop {
            if let Err(e) = listen_once(&conn_string, &channels, &handler).await {
                eprintln!("[{label}] LISTEN connection ended: {e}; reconnecting in 2s");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

async fn listen_once<F>(
    conn_string: &str,
    channels: &[String],
    handler: &Arc<F>,
) -> Result<(), String>
where
    F: Fn(&str, &str) + Send + Sync + 'static,
{
    use futures::{StreamExt, stream};
    use tokio_postgres::{AsyncMessage, NoTls, connect};

    let (client, mut connection) = connect(conn_string, NoTls)
        .await
        .map_err(|e| e.to_string())?;

    // Driving the connection's message stream performs the socket I/O (which is
    // also what lets the LISTEN below complete) and yields async notifications.
    let handler = Arc::clone(handler);
    let conn_task = tokio::spawn(async move {
        let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(AsyncMessage::Notification(note)) => handler(note.channel(), note.payload()),
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    for ch in channels {
        client
            .batch_execute(&format!("LISTEN {ch}"))
            .await
            .map_err(|e| e.to_string())?;
    }

    // Keep `client` alive until the connection task ends (dropping it closes the
    // connection), then let the caller reconnect.
    let result = conn_task.await.map_err(|e| e.to_string());
    drop(client);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub() -> Arc<KeyedBroadcastHub<String, i32>> {
        Arc::new(KeyedBroadcastHub::new(8, 16))
    }

    #[test]
    fn release_reaps_only_after_last_receiver_drops() {
        let h = hub();
        let rx = h.subscribe(&"a".into());
        assert_eq!(h.entry_count(), 1);
        h.release(&"a".into()); // a live receiver remains
        assert_eq!(h.entry_count(), 1);
        drop(rx);
        h.release(&"a".into());
        assert_eq!(h.entry_count(), 0);
    }

    #[test]
    fn publish_reaps_when_no_receivers() {
        let h = hub();
        drop(h.subscribe(&"a".into()));
        h.publish(&"a".into(), 1);
        assert_eq!(h.entry_count(), 0);
    }

    #[tokio::test]
    async fn publish_delivers_and_keeps_entry_with_live_receiver() {
        let h = hub();
        let mut rx = h.subscribe(&"a".into());
        h.publish(&"a".into(), 42);
        assert_eq!(h.entry_count(), 1);
        assert_eq!(rx.recv().await.unwrap(), 42);
    }

    #[test]
    fn guard_reaps_on_drop() {
        let h = hub();
        let sub = h.subscribe_guarded(&"a".into());
        assert_eq!(h.entry_count(), 1);
        drop(sub);
        assert_eq!(h.entry_count(), 0);
    }

    #[test]
    fn distinct_keys_spread_across_shards() {
        let h = hub();
        let _rxs: Vec<_> = ["a", "b", "c", "d", "e", "f", "g", "h"]
            .iter()
            .map(|k| h.subscribe(&k.to_string()))
            .collect();
        assert!(h.occupied_shards() > 1, "keys should spread across shards");
        assert_eq!(h.entry_count(), 8);
    }
}
