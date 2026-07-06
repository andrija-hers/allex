//! A keyed, awaitable fetch-coalescing cache.
//!
//! [`AwaitableMap`] resolves keys through a caller-supplied
//! [`AwaitableMapFetcher`], with:
//! - **single-flight coalescing** — concurrent requests for a key in flight
//!   await the same fetch (per-key `watch` channels, [`PendingEntry`]);
//! - **pluggable persistence** — [`AwaitableMapPersistence`], with bundled
//!   SQLite and in-memory implementations;
//! - **failure cooldown** — a failed key is not re-fetched until the cooldown
//!   elapses;
//! - **progress reporting** — fetchers can stream progress via
//!   [`ProgressableAwaitableMapFetcher`] / `ProgressableFuture`.
//!
//! [`DailyAwaitableMap`] partitions the cache by `YYYYMMDD` date with one
//! SQLite database per day.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::NaiveDate;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use progressable_future::{ProgressableFuture, Step, TransportIssue};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::watch;

const DEFAULT_DAILY_ROOT_DIRECTORY: &str = ".awaitable-map";
const DAILY_DATABASE_SUBDIRECTORY: &str = "ds";
const DEFAULT_FAILURE_COOLDOWN: Duration = Duration::from_secs(30);

pub trait AwaitableMapPersistence<K, V> {
    type Error;

    fn load_many(&self, keys: &[K]) -> Result<HashMap<K, V>, Self::Error>;
    fn persist_many(&self, entries: &[(K, V)]) -> Result<(), Self::Error>;
    /// Remove the given keys from the persisted cache. A key that is not
    /// present is silently ignored. Used to force re-extraction of entries that
    /// failed downstream validation.
    fn delete_many(&self, keys: &[K]) -> Result<(), Self::Error>;
}

// Internal trait; the auto-trait-bound caveat of `async fn` in traits doesn't
// apply to these in-crate impls.
#[allow(async_fn_in_trait)]
pub trait AwaitableMapFetcher<K, V> {
    type Error;

    async fn fetch_many(&self, keys: &[K]) -> Result<Vec<(K, V)>, Self::Error>;
}

pub trait ProgressableAwaitableMapFetcher<K, V>: AwaitableMapFetcher<K, V> {
    type Progress;

    fn fetch_many_progressable(
        &self,
        keys: &[K],
    ) -> ProgressableFuture<Self::Progress, Vec<(K, V)>, Self::Error>;
}

pub struct AwaitableMap<K, V, P, F> {
    persistence: P,
    fetcher: F,
    marker: PhantomData<(K, V)>,
}

impl<K, V, P, F> AwaitableMap<K, V, P, F> {
    pub fn new(persistence: P, fetcher: F) -> Self {
        Self {
            persistence,
            fetcher,
            marker: PhantomData,
        }
    }
}

impl<K, V, P, F> AwaitableMap<K, V, P, F>
where
    K: Clone + Eq + Hash,
    V: Clone,
    P: AwaitableMapPersistence<K, V>,
    F: AwaitableMapFetcher<K, V>,
{
    pub async fn query(&self, keys: &[K]) -> Result<Vec<V>, AwaitableMapError<P::Error, F::Error>> {
        let unique_keys = ordered_unique(keys);
        let mut resolved: HashMap<K, Arc<V>> = self
            .persistence
            .load_many(&unique_keys)
            .map_err(AwaitableMapError::PersistenceLoad)?
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let missing_keys: Vec<K> = unique_keys
            .iter()
            .filter(|key| !resolved.contains_key(*key))
            .cloned()
            .collect();

        if !missing_keys.is_empty() {
            let fetched = self
                .fetcher
                .fetch_many(&missing_keys)
                .await
                .map_err(AwaitableMapError::Fetch)?;

            let mut fetched_map = HashMap::with_capacity(fetched.len());
            for (key, value) in &fetched {
                if fetched_map
                    .insert(key.clone(), Arc::new(value.clone()))
                    .is_some()
                {
                    return Err(AwaitableMapError::DuplicateFetchedKey);
                }
            }

            if missing_keys
                .iter()
                .any(|key| !fetched_map.contains_key(key))
            {
                return Err(AwaitableMapError::MissingFetchedKeys);
            }

            self.persistence
                .persist_many(&fetched)
                .map_err(AwaitableMapError::PersistenceStore)?;
            resolved.extend(fetched_map);
        }

        keys.iter()
            .map(|key| {
                resolved
                    .get(key)
                    .cloned()
                    .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone()))
                    .ok_or(AwaitableMapError::MissingFetchedKeys)
            })
            .collect()
    }
}

#[derive(Debug, Error)]
pub enum AwaitableMapError<P, F> {
    #[error("could not load persisted entries: {0}")]
    PersistenceLoad(P),
    #[error("could not persist fetched entries: {0}")]
    PersistenceStore(P),
    #[error("could not fetch missing entries: {0}")]
    Fetch(F),
    #[error("progressable fetch was interrupted before completion")]
    FetchInterrupted,
    #[error("shared fetch cooldown active for another {remaining:?}")]
    FetchCooldown { remaining: Duration },
    #[error("fetcher returned duplicate results for the same key")]
    DuplicateFetchedKey,
    #[error("fetcher did not return all requested keys")]
    MissingFetchedKeys,
}

/// A watch-channel entry representing an in-flight fetch for a single key.
/// Other tasks can subscribe to receive the result once the fetch completes.
pub struct PendingEntry<V> {
    pub extraction_id: u64,
    started_at: std::time::Instant,
    sender: watch::Sender<Option<Arc<V>>>,
}

impl<V> PendingEntry<V> {
    pub fn new(extraction_id: u64) -> Self {
        let (sender, _) = watch::channel(None);
        Self {
            extraction_id,
            started_at: std::time::Instant::now(),
            sender,
        }
    }

    fn subscribe(&self) -> watch::Receiver<Option<Arc<V>>> {
        self.sender.subscribe()
    }

    /// Resolve the pending entry by broadcasting the fetched value to all waiters.
    pub fn resolve(&self, value: Arc<V>) {
        let _ = self.sender.send(Some(value));
    }

    pub fn duration(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

/// The outcome of a `DailyAwaitableMap::query` call.
pub struct QueryOutcome<V> {
    pub values: Vec<V>,
    /// Extraction IDs that this query observed as in-flight (started by another task)
    /// and waited for.
    pub awaited_extraction_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryProgress<P> {
    AwaitingExtractions { extraction_ids: Vec<u64> },
    Fetch(P),
}

enum PendingWaitOutcome<K, V> {
    Resolved {
        key: K,
        extraction_id: u64,
        value: Arc<V>,
    },
    Retry {
        key: K,
    },
}

async fn wait_for_pending_values<K, V>(
    pending_waiters: Vec<(K, u64, watch::Receiver<Option<Arc<V>>>)>,
) -> Vec<PendingWaitOutcome<K, V>>
where
    V: Clone,
{
    let mut outcomes = Vec::with_capacity(pending_waiters.len());
    for (key, extraction_id, mut rx) in pending_waiters {
        match rx.wait_for(|v| v.is_some()).await {
            Ok(guard) => {
                let value_arc = guard
                    .as_ref()
                    .expect("value is Some after wait_for")
                    .clone();
                drop(guard);
                outcomes.push(PendingWaitOutcome::Resolved {
                    key,
                    extraction_id,
                    value: value_arc,
                });
            }
            Err(_) => outcomes.push(PendingWaitOutcome::Retry { key }),
        }
    }
    outcomes
}

pub struct DailyAwaitableMap<K, V, F> {
    date: String,
    database_path: Option<PathBuf>,
    persistence: MapPersistence<K, V>,
    fetcher: F,
    pending: Arc<Mutex<HashMap<K, PendingEntry<V>>>>,
    failures: Arc<Mutex<HashMap<K, Instant>>>,
}

impl<K, V, F> DailyAwaitableMap<K, V, F>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
    F: AwaitableMapFetcher<K, V>,
{
    pub fn new(date: &str, fetcher: F) -> Result<Self, SqliteMapError> {
        let root_directory = std::env::current_dir()?.join(DEFAULT_DAILY_ROOT_DIRECTORY);
        Self::with_root_directory(date, root_directory, fetcher)
    }

    pub fn with_root_directory(
        date: &str,
        root_directory: impl AsRef<Path>,
        fetcher: F,
    ) -> Result<Self, SqliteMapError> {
        let date = validate_date(date)?;
        let database_path = daily_database_path(root_directory, &date);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let failures = Arc::new(Mutex::new(HashMap::new()));
        Self::with_database_path_and_retry_state(
            &date,
            database_path,
            fetcher,
            pending,
            failures,
            DEFAULT_FAILURE_COOLDOWN,
        )
    }

    pub fn with_database_path(
        date: &str,
        database_path: impl AsRef<Path>,
        fetcher: F,
        pending: Arc<Mutex<HashMap<K, PendingEntry<V>>>>,
    ) -> Result<Self, SqliteMapError> {
        let failures = Arc::new(Mutex::new(HashMap::new()));
        Self::with_database_path_and_retry_state(
            date,
            database_path,
            fetcher,
            pending,
            failures,
            DEFAULT_FAILURE_COOLDOWN,
        )
    }

    pub fn with_database_path_and_retry_state(
        date: &str,
        database_path: impl AsRef<Path>,
        fetcher: F,
        pending: Arc<Mutex<HashMap<K, PendingEntry<V>>>>,
        failures: Arc<Mutex<HashMap<K, Instant>>>,
        _failure_cooldown: Duration,
    ) -> Result<Self, SqliteMapError> {
        let date = validate_date(date)?;
        let database_path = database_path.as_ref().to_path_buf();
        let persistence = MapPersistence::Sqlite(SqliteMapPersistence::new(&database_path)?);

        Ok(Self {
            date,
            database_path: Some(database_path),
            persistence,
            fetcher,
            pending,
            failures,
        })
    }

    pub fn with_retry_state_in_memory(
        date: &str,
        fetcher: F,
        pending: Arc<Mutex<HashMap<K, PendingEntry<V>>>>,
        failures: Arc<Mutex<HashMap<K, Instant>>>,
        _failure_cooldown: Duration,
    ) -> Result<Self, SqliteMapError> {
        let date = validate_date(date)?;
        let persistence = MapPersistence::Memory(MemoryMapPersistence::default());

        Ok(Self {
            date,
            database_path: None,
            persistence,
            fetcher,
            pending,
            failures,
        })
    }

    pub fn date(&self) -> &str {
        &self.date
    }

    pub fn database_path(&self) -> Option<&Path> {
        self.database_path.as_deref()
    }

    /// Returns the number of keys from `keys` that are already stored in the
    /// persistence database.  Useful for detecting "served from cache" drives
    /// where all instruments were resolved from sqlite without triggering a new
    /// Datascope extraction.
    pub fn count_cached(&self, keys: &[K]) -> Result<usize, SqliteMapError> {
        let unique = ordered_unique(keys);
        let resolved = self.persistence.load_many(&unique)?;
        Ok(resolved.len())
    }

    /// Remove `keys` from the persisted cache so a subsequent query re-extracts
    /// them. Keys not present are ignored. This only touches persistence; any
    /// in-flight `pending` waiters are left untouched (they are already
    /// resolved by the time eviction runs).
    pub fn delete_many(&self, keys: &[K]) -> Result<(), SqliteMapError> {
        let unique = ordered_unique(keys);
        self.persistence.delete_many(&unique)
    }

    pub fn pending_map(&self) -> Arc<Mutex<HashMap<K, PendingEntry<V>>>> {
        Arc::clone(&self.pending)
    }

    fn cooldown_remaining_for(&self, key: &K) -> Option<Duration> {
        let now = Instant::now();
        let mut failures = self.failures.lock().unwrap();
        match failures.get(key).copied() {
            Some(until) if until > now => Some(until.duration_since(now)),
            Some(_) => {
                failures.remove(key);
                None
            }
            None => None,
        }
    }

    pub async fn query(
        &self,
        keys: &[K],
    ) -> Result<QueryOutcome<V>, AwaitableMapError<SqliteMapError, F::Error>> {
        let unique_keys = ordered_unique(keys);
        let mut resolved: HashMap<K, Arc<V>> = self
            .persistence
            .load_many(&unique_keys)
            .map_err(AwaitableMapError::PersistenceLoad)?
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let mut awaited_extraction_ids: Vec<u64> = Vec::new();

        let mut remaining: Vec<K> = unique_keys
            .iter()
            .filter(|key| !resolved.contains_key(*key))
            .cloned()
            .collect();

        while !remaining.is_empty() {
            // Split remaining into pending waiters and truly unknown — done
            // entirely before any await so no RefCell borrow is held across a
            // yield point.
            let mut pending_waiters: Vec<(K, u64, watch::Receiver<Option<Arc<V>>>)> = Vec::new();
            let mut truly_unknown: Vec<K> = Vec::new();
            {
                let pending = self.pending.lock().unwrap();
                for key in &remaining {
                    if let Some(entry) = pending.get(key) {
                        let rx = entry.subscribe();
                        pending_waiters.push((key.clone(), entry.extraction_id, rx));
                    } else {
                        truly_unknown.push(key.clone());
                    }
                }
            }

            remaining = Vec::new();

            let (pending_outcomes, fetched_result) =
                tokio::join!(wait_for_pending_values(pending_waiters), async {
                    if truly_unknown.is_empty() {
                        Ok(Vec::new())
                    } else {
                        self.fetcher.fetch_many(&truly_unknown).await
                    }
                });

            for outcome in pending_outcomes {
                match outcome {
                    PendingWaitOutcome::Resolved {
                        key,
                        extraction_id,
                        value,
                    } => {
                        resolved.insert(key, value);
                        awaited_extraction_ids.push(extraction_id);
                    }
                    PendingWaitOutcome::Retry { key } => {
                        if let Some(remaining) = self.cooldown_remaining_for(&key) {
                            return Err(AwaitableMapError::FetchCooldown { remaining });
                        }
                        remaining.push(key);
                    }
                }
            }

            if !truly_unknown.is_empty() {
                let fetched = fetched_result.map_err(AwaitableMapError::Fetch)?;

                let mut fetched_map = HashMap::with_capacity(fetched.len());
                for (key, value) in &fetched {
                    if fetched_map
                        .insert(key.clone(), Arc::new(value.clone()))
                        .is_some()
                    {
                        return Err(AwaitableMapError::DuplicateFetchedKey);
                    }
                }

                if truly_unknown
                    .iter()
                    .any(|key| !fetched_map.contains_key(key))
                {
                    return Err(AwaitableMapError::MissingFetchedKeys);
                }

                self.persistence
                    .persist_many(&fetched)
                    .map_err(AwaitableMapError::PersistenceStore)?;
                resolved.extend(fetched_map);
            }
        }

        let values = keys
            .iter()
            .map(|key| {
                resolved
                    .get(key)
                    .cloned()
                    .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone()))
                    .ok_or(AwaitableMapError::MissingFetchedKeys)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QueryOutcome {
            values,
            awaited_extraction_ids,
        })
    }
}

impl<K, V, F> DailyAwaitableMap<K, V, F>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
    F: ProgressableAwaitableMapFetcher<K, V>,
{
    pub async fn query_with_progress<P>(
        &self,
        keys: &[K],
        mut on_progress: impl FnMut(P),
    ) -> Result<QueryOutcome<V>, AwaitableMapError<SqliteMapError, F::Error>>
    where
        F: ProgressableAwaitableMapFetcher<K, V, Progress = P>,
        F::Error: Send + 'static,
        P: Send + 'static,
    {
        self.query_with_observed_progress(keys, |progress| {
            if let QueryProgress::Fetch(progress) = progress {
                on_progress(progress);
            }
        })
        .await
    }

    pub async fn query_with_observed_progress<P>(
        &self,
        keys: &[K],
        mut on_progress: impl FnMut(QueryProgress<P>),
    ) -> Result<QueryOutcome<V>, AwaitableMapError<SqliteMapError, F::Error>>
    where
        F: ProgressableAwaitableMapFetcher<K, V, Progress = P>,
        F::Error: Send + 'static,
        P: Send + 'static,
    {
        let unique_keys = ordered_unique(keys);
        let mut resolved: HashMap<K, Arc<V>> = self
            .persistence
            .load_many(&unique_keys)
            .map_err(AwaitableMapError::PersistenceLoad)?
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let mut awaited_extraction_ids: Vec<u64> = Vec::new();

        let mut remaining: Vec<K> = unique_keys
            .iter()
            .filter(|key| !resolved.contains_key(*key))
            .cloned()
            .collect();

        while !remaining.is_empty() {
            let mut pending_waiters: Vec<(K, u64, watch::Receiver<Option<Arc<V>>>)> = Vec::new();
            let mut truly_unknown: Vec<K> = Vec::new();
            {
                let pending = self.pending.lock().unwrap();
                for key in &remaining {
                    if let Some(entry) = pending.get(key) {
                        let rx = entry.subscribe();
                        pending_waiters.push((key.clone(), entry.extraction_id, rx));
                    } else if let Some(remaining) = self.cooldown_remaining_for(key) {
                        return Err(AwaitableMapError::FetchCooldown { remaining });
                    } else {
                        truly_unknown.push(key.clone());
                    }
                }
            }

            if !pending_waiters.is_empty() {
                let mut extraction_ids: Vec<u64> = pending_waiters
                    .iter()
                    .map(|(_, extraction_id, _)| *extraction_id)
                    .collect();
                extraction_ids.sort_unstable();
                extraction_ids.dedup();
                on_progress(QueryProgress::AwaitingExtractions { extraction_ids });
            }

            remaining = Vec::new();

            let mut pending_waits = FuturesUnordered::new();
            for (key, extraction_id, mut rx) in pending_waiters {
                pending_waits.push(async move {
                    match rx.wait_for(|v| v.is_some()).await {
                        Ok(guard) => {
                            let value_arc = guard
                                .as_ref()
                                .expect("value is Some after wait_for")
                                .clone();
                            drop(guard);
                            PendingWaitOutcome::Resolved {
                                key,
                                extraction_id,
                                value: value_arc,
                            }
                        }
                        Err(_) => PendingWaitOutcome::Retry { key },
                    }
                });
            }

            let mut fetch_stream = if truly_unknown.is_empty() {
                None
            } else {
                Some(Box::pin(
                    self.fetcher
                        .fetch_many_progressable(&truly_unknown)
                        .stream(),
                ))
            };

            if !truly_unknown.is_empty() {
                while !pending_waits.is_empty() || fetch_stream.is_some() {
                    tokio::select! {
                        Some(outcome) = pending_waits.next(), if !pending_waits.is_empty() => {
                            match outcome {
                                PendingWaitOutcome::Resolved {
                                    key,
                                    extraction_id,
                                    value,
                                } => {
                                    resolved.insert(key, value);
                                    awaited_extraction_ids.push(extraction_id);
                                }
                                PendingWaitOutcome::Retry { key } => {
                                    if let Some(remaining) = self.cooldown_remaining_for(&key) {
                                        return Err(AwaitableMapError::FetchCooldown { remaining });
                                    }
                                    remaining.push(key);
                                }
                            }
                        }
                        step = async {
                            match fetch_stream.as_mut() {
                                Some(stream) => stream.next().await,
                                None => None,
                            }
                        }, if fetch_stream.is_some() => {
                            match step {
                                Some(Step::Progress(progress)) => {
                                    on_progress(QueryProgress::Fetch(progress));
                                }
                                Some(Step::Success(fetched)) => {
                                    let mut fetched_map = HashMap::with_capacity(fetched.len());
                                    for (key, value) in &fetched {
                                        if fetched_map.insert(key.clone(), Arc::new(value.clone())).is_some() {
                                            return Err(AwaitableMapError::DuplicateFetchedKey);
                                        }
                                    }

                                    if truly_unknown
                                        .iter()
                                        .any(|key| !fetched_map.contains_key(key))
                                    {
                                        return Err(AwaitableMapError::MissingFetchedKeys);
                                    }

                                    self.persistence
                                        .persist_many(&fetched)
                                        .map_err(AwaitableMapError::PersistenceStore)?;
                                    resolved.extend(fetched_map);
                                    fetch_stream = None;
                                }
                                Some(Step::Error(error)) => return Err(AwaitableMapError::Fetch(error)),
                                Some(Step::Transport(TransportIssue::Interrupted)) | None => {
                                    return Err(AwaitableMapError::FetchInterrupted);
                                }
                                Some(Step::Transport(TransportIssue::Error(_))) => {
                                    return Err(AwaitableMapError::FetchInterrupted);
                                }
                            }
                        }
                    }
                }
            } else {
                while let Some(outcome) = pending_waits.next().await {
                    match outcome {
                        PendingWaitOutcome::Resolved {
                            key,
                            extraction_id,
                            value,
                        } => {
                            resolved.insert(key, value);
                            awaited_extraction_ids.push(extraction_id);
                        }
                        PendingWaitOutcome::Retry { key } => {
                            if let Some(remaining) = self.cooldown_remaining_for(&key) {
                                return Err(AwaitableMapError::FetchCooldown { remaining });
                            }
                            remaining.push(key);
                        }
                    }
                }
            }
        }

        let values = keys
            .iter()
            .map(|key| {
                resolved
                    .get(key)
                    .cloned()
                    .map(|arc| Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone()))
                    .ok_or(AwaitableMapError::MissingFetchedKeys)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QueryOutcome {
            values,
            awaited_extraction_ids,
        })
    }
}

struct SqliteMapPersistence<K, V> {
    // The database connection is kept open and shared behind a mutex.
    // Opening a new handle for every load_many/persist_many call (the old behaviour)
    // produced one blocking open per schedule per drive cycle — ~240 redundant
    // file-descriptor acquires that each took several milliseconds and filled
    // the 500 ms batch-coalescing window before most schedules could enrol.
    database: Arc<Mutex<Connection>>,
    marker: PhantomData<(K, V)>,
}

struct MemoryMapPersistence<K, V> {
    entries: Arc<Mutex<HashMap<K, V>>>,
}

impl<K, V> Default for MemoryMapPersistence<K, V> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

enum MapPersistence<K, V> {
    Sqlite(SqliteMapPersistence<K, V>),
    Memory(MemoryMapPersistence<K, V>),
}

impl<K, V> SqliteMapPersistence<K, V> {
    fn new(path: impl AsRef<Path>) -> Result<Self, SqliteMapError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let database = Connection::open(path).map_err(sqlite_error)?;
        database
            .pragma_update(None, "mmap_size", 0_i64)
            .map_err(sqlite_error)?;
        database
            .execute(
                "CREATE TABLE IF NOT EXISTS entries (
                    storage_key TEXT PRIMARY KEY,
                    value BLOB NOT NULL
                ) WITHOUT ROWID",
                [],
            )
            .map_err(sqlite_error)?;

        Ok(Self {
            database: Arc::new(Mutex::new(database)),
            marker: PhantomData,
        })
    }
}

impl<K, V> AwaitableMapPersistence<K, V> for SqliteMapPersistence<K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    type Error = SqliteMapError;

    fn load_many(&self, keys: &[K]) -> Result<HashMap<K, V>, Self::Error> {
        let connection = self.database.lock().unwrap();
        let mut query = connection
            .prepare("SELECT value FROM entries WHERE storage_key = ?1")
            .map_err(sqlite_error)?;
        let mut resolved = HashMap::with_capacity(keys.len());

        for key in keys {
            let storage_key = storage_key(key)?;
            let encoded: Option<Vec<u8>> = query
                .query_row(params![storage_key], |row| row.get(0))
                .optional()
                .map_err(sqlite_error)?;
            if let Some(value) = encoded {
                let decoded = serde_json::from_slice(&value)?;
                resolved.insert(key.clone(), decoded);
            }
        }

        Ok(resolved)
    }

    fn persist_many(&self, entries: &[(K, V)]) -> Result<(), Self::Error> {
        let mut connection = self.database.lock().unwrap();
        let transaction = connection.transaction().map_err(sqlite_error)?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO entries(storage_key, value) VALUES (?1, ?2)
                     ON CONFLICT(storage_key) DO UPDATE SET value = excluded.value",
                )
                .map_err(sqlite_error)?;
            for (key, value) in entries {
                let storage_key = storage_key(key)?;
                let encoded = serde_json::to_vec(value)?;
                statement
                    .execute(params![storage_key, encoded])
                    .map_err(sqlite_error)?;
            }
        }
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }

    fn delete_many(&self, keys: &[K]) -> Result<(), Self::Error> {
        let mut connection = self.database.lock().unwrap();
        let transaction = connection.transaction().map_err(sqlite_error)?;
        {
            let mut statement = transaction
                .prepare("DELETE FROM entries WHERE storage_key = ?1")
                .map_err(sqlite_error)?;
            for key in keys {
                let storage_key = storage_key(key)?;
                statement
                    .execute(params![storage_key])
                    .map_err(sqlite_error)?;
            }
        }
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }
}

impl<K, V> AwaitableMapPersistence<K, V> for MemoryMapPersistence<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Error = SqliteMapError;

    fn load_many(&self, keys: &[K]) -> Result<HashMap<K, V>, Self::Error> {
        let entries = self.entries.lock().unwrap();
        let mut resolved = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = entries.get(key) {
                resolved.insert(key.clone(), value.clone());
            }
        }
        Ok(resolved)
    }

    fn persist_many(&self, entries: &[(K, V)]) -> Result<(), Self::Error> {
        let mut persisted = self.entries.lock().unwrap();
        for (key, value) in entries {
            persisted.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    fn delete_many(&self, keys: &[K]) -> Result<(), Self::Error> {
        let mut persisted = self.entries.lock().unwrap();
        for key in keys {
            persisted.remove(key);
        }
        Ok(())
    }
}

impl<K, V> AwaitableMapPersistence<K, V> for MapPersistence<K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    type Error = SqliteMapError;

    fn load_many(&self, keys: &[K]) -> Result<HashMap<K, V>, Self::Error> {
        match self {
            Self::Sqlite(persistence) => persistence.load_many(keys),
            Self::Memory(persistence) => persistence.load_many(keys),
        }
    }

    fn persist_many(&self, entries: &[(K, V)]) -> Result<(), Self::Error> {
        match self {
            Self::Sqlite(persistence) => persistence.persist_many(entries),
            Self::Memory(persistence) => persistence.persist_many(entries),
        }
    }

    fn delete_many(&self, keys: &[K]) -> Result<(), Self::Error> {
        match self {
            Self::Sqlite(persistence) => persistence.delete_many(keys),
            Self::Memory(persistence) => persistence.delete_many(keys),
        }
    }
}

#[derive(Debug, Error)]
pub enum SqliteMapError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("sqlite error: {0}")]
    Sqlite(String),
    #[error("date must be in YYYYMMDD format, got {0:?}")]
    InvalidDate(String),
}

/// First-occurrence, order-preserving dedup.
pub fn ordered_unique<K>(keys: &[K]) -> Vec<K>
where
    K: Clone + Eq + Hash,
{
    let mut seen = HashSet::with_capacity(keys.len());
    let mut unique = Vec::with_capacity(keys.len());

    for key in keys {
        if seen.insert(key.clone()) {
            unique.push(key.clone());
        }
    }

    unique
}

fn validate_date(date: &str) -> Result<String, SqliteMapError> {
    NaiveDate::parse_from_str(date, "%Y%m%d")
        .map(|_| date.to_owned())
        .map_err(|_| SqliteMapError::InvalidDate(date.to_owned()))
}

fn storage_key<K>(key: &K) -> Result<String, SqliteMapError>
where
    K: Serialize,
{
    Ok(serde_json::to_string(key)?)
}

fn daily_database_path(root_directory: impl AsRef<Path>, date: &str) -> PathBuf {
    root_directory
        .as_ref()
        .join(DAILY_DATABASE_SUBDIRECTORY)
        .join(date)
        .join(format!("{date}.sqlite"))
}

fn sqlite_error(error: impl std::fmt::Display) -> SqliteMapError {
    SqliteMapError::Sqlite(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    /// Test key mirroring a typical composite domain key.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct InstrumentDuplet {
        instrument_type: String,
        instrument_code: String,
    }

    impl InstrumentDuplet {
        fn new(instrument_type: impl Into<String>, instrument_code: impl Into<String>) -> Self {
            Self {
                instrument_type: instrument_type.into(),
                instrument_code: instrument_code.into(),
            }
        }
    }

    #[derive(Default)]
    struct MemoryPersistence {
        stored: RefCell<HashMap<InstrumentDuplet, String>>,
        persisted_batches: RefCell<Vec<Vec<(InstrumentDuplet, String)>>>,
    }

    impl AwaitableMapPersistence<InstrumentDuplet, String> for MemoryPersistence {
        type Error = &'static str;

        fn load_many(
            &self,
            keys: &[InstrumentDuplet],
        ) -> Result<HashMap<InstrumentDuplet, String>, Self::Error> {
            let stored = self.stored.borrow();
            Ok(keys
                .iter()
                .filter_map(|key| stored.get(key).cloned().map(|value| (key.clone(), value)))
                .collect())
        }

        fn persist_many(&self, entries: &[(InstrumentDuplet, String)]) -> Result<(), Self::Error> {
            self.persisted_batches.borrow_mut().push(entries.to_vec());
            let mut stored = self.stored.borrow_mut();
            for (key, value) in entries {
                stored.insert(key.clone(), value.clone());
            }
            Ok(())
        }

        fn delete_many(&self, keys: &[InstrumentDuplet]) -> Result<(), Self::Error> {
            let mut stored = self.stored.borrow_mut();
            for key in keys {
                stored.remove(key);
            }
            Ok(())
        }
    }

    struct MemoryFetcher {
        responses: HashMap<InstrumentDuplet, String>,
        fetch_count: Cell<usize>,
        omit_last: bool,
    }

    impl AwaitableMapFetcher<InstrumentDuplet, String> for MemoryFetcher {
        type Error = &'static str;

        async fn fetch_many(
            &self,
            keys: &[InstrumentDuplet],
        ) -> Result<Vec<(InstrumentDuplet, String)>, Self::Error> {
            self.fetch_count.set(self.fetch_count.get() + 1);

            let mut results = Vec::new();
            for (index, key) in keys.iter().enumerate() {
                if self.omit_last && index + 1 == keys.len() {
                    break;
                }
                if let Some(value) = self.responses.get(key) {
                    results.push((key.clone(), value.clone()));
                }
            }
            Ok(results)
        }
    }

    struct ProgressMemoryFetcher {
        responses: HashMap<InstrumentDuplet, String>,
        started_ids: Arc<Mutex<Vec<u64>>>,
    }

    impl AwaitableMapFetcher<InstrumentDuplet, String> for ProgressMemoryFetcher {
        type Error = &'static str;

        async fn fetch_many(
            &self,
            keys: &[InstrumentDuplet],
        ) -> Result<Vec<(InstrumentDuplet, String)>, Self::Error> {
            let mut results = Vec::new();
            for key in keys {
                if let Some(value) = self.responses.get(key) {
                    results.push((key.clone(), value.clone()));
                }
            }
            Ok(results)
        }
    }

    impl ProgressableAwaitableMapFetcher<InstrumentDuplet, String> for ProgressMemoryFetcher {
        type Progress = u64;

        fn fetch_many_progressable(
            &self,
            keys: &[InstrumentDuplet],
        ) -> ProgressableFuture<Self::Progress, Vec<(InstrumentDuplet, String)>, Self::Error>
        {
            let responses = self.responses.clone();
            let started_ids = self.started_ids.clone();
            let keys = keys.to_vec();
            ProgressableFuture::new(move |sender| async move {
                let extraction_id = started_ids.lock().unwrap().len() as u64 + 1;
                started_ids.lock().unwrap().push(extraction_id);
                sender.progress(extraction_id);
                let mut results = Vec::new();
                for key in &keys {
                    if let Some(value) = responses.get(key) {
                        results.push((key.clone(), value.clone()));
                    }
                }
                Ok(results)
            })
        }
    }

    struct FailingProgressMemoryFetcher {
        fetch_count: Arc<AtomicUsize>,
        failures: Arc<Mutex<HashMap<InstrumentDuplet, Instant>>>,
        failure_cooldown: Duration,
    }

    struct CoordinatedProgressMemoryFetcher {
        responses: HashMap<InstrumentDuplet, String>,
        fetch_started: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        release_fetch: Arc<tokio::sync::Notify>,
    }

    impl AwaitableMapFetcher<InstrumentDuplet, String> for FailingProgressMemoryFetcher {
        type Error = &'static str;

        async fn fetch_many(
            &self,
            keys: &[InstrumentDuplet],
        ) -> Result<Vec<(InstrumentDuplet, String)>, Self::Error> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            let retry_at = Instant::now() + self.failure_cooldown;
            let mut failures = self.failures.lock().unwrap();
            for key in keys {
                failures.insert(key.clone(), retry_at);
            }
            Err("boom")
        }
    }

    impl ProgressableAwaitableMapFetcher<InstrumentDuplet, String> for FailingProgressMemoryFetcher {
        type Progress = u64;

        fn fetch_many_progressable(
            &self,
            keys: &[InstrumentDuplet],
        ) -> ProgressableFuture<Self::Progress, Vec<(InstrumentDuplet, String)>, Self::Error>
        {
            let fetch_count = self.fetch_count.clone();
            let failures = self.failures.clone();
            let failure_cooldown = self.failure_cooldown;
            let keys = keys.to_vec();
            ProgressableFuture::new(move |_| async move {
                fetch_count.fetch_add(1, Ordering::SeqCst);
                let retry_at = Instant::now() + failure_cooldown;
                let mut failures = failures.lock().unwrap();
                for key in &keys {
                    failures.insert(key.clone(), retry_at);
                }
                Err("boom")
            })
        }
    }

    impl AwaitableMapFetcher<InstrumentDuplet, String> for CoordinatedProgressMemoryFetcher {
        type Error = &'static str;

        async fn fetch_many(
            &self,
            keys: &[InstrumentDuplet],
        ) -> Result<Vec<(InstrumentDuplet, String)>, Self::Error> {
            let mut results = Vec::new();
            for key in keys {
                if let Some(value) = self.responses.get(key) {
                    results.push((key.clone(), value.clone()));
                }
            }
            Ok(results)
        }
    }

    impl ProgressableAwaitableMapFetcher<InstrumentDuplet, String>
        for CoordinatedProgressMemoryFetcher
    {
        type Progress = u64;

        fn fetch_many_progressable(
            &self,
            keys: &[InstrumentDuplet],
        ) -> ProgressableFuture<Self::Progress, Vec<(InstrumentDuplet, String)>, Self::Error>
        {
            let responses = self.responses.clone();
            let fetch_started = self.fetch_started.clone();
            let release_fetch = self.release_fetch.clone();
            let keys = keys.to_vec();
            ProgressableFuture::new(move |sender| async move {
                if let Some(tx) = fetch_started.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                sender.progress(7);
                release_fetch.notified().await;
                let mut results = Vec::new();
                for key in &keys {
                    if let Some(value) = responses.get(key) {
                        results.push((key.clone(), value.clone()));
                    }
                }
                Ok(results)
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_uses_persisted_values_before_fetching() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let msft = InstrumentDuplet::new("Ric", "MSFT.O");
        let persistence = MemoryPersistence::default();
        persistence
            .stored
            .borrow_mut()
            .insert(apple.clone(), "cached-apple".to_string());
        persistence
            .stored
            .borrow_mut()
            .insert(msft.clone(), "cached-msft".to_string());
        let fetcher = MemoryFetcher {
            responses: HashMap::new(),
            fetch_count: Cell::new(0),
            omit_last: false,
        };

        let map = AwaitableMap::new(persistence, fetcher);
        let result = map
            .query(&[apple.clone(), msft.clone(), apple.clone()])
            .await
            .expect("query should succeed");

        assert_eq!(result, vec!["cached-apple", "cached-msft", "cached-apple"]);
        assert_eq!(map.fetcher.fetch_count.get(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_fetches_missing_values_once_and_persists_them() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let msft = InstrumentDuplet::new("Ric", "MSFT.O");
        let persistence = MemoryPersistence::default();
        persistence
            .stored
            .borrow_mut()
            .insert(apple.clone(), "cached-apple".to_string());
        let fetcher = MemoryFetcher {
            responses: HashMap::from([(msft.clone(), "fetched-msft".to_string())]),
            fetch_count: Cell::new(0),
            omit_last: false,
        };

        let map = AwaitableMap::new(persistence, fetcher);
        let result = map
            .query(&[msft.clone(), apple.clone(), msft.clone()])
            .await
            .expect("query should succeed");

        assert_eq!(result, vec!["fetched-msft", "cached-apple", "fetched-msft"]);
        assert_eq!(map.fetcher.fetch_count.get(), 1);
        assert_eq!(
            map.persistence.persisted_batches.borrow().as_slice(),
            &[vec![(msft, "fetched-msft".to_string())]]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_fails_when_fetcher_does_not_return_every_missing_key() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let msft = InstrumentDuplet::new("Ric", "MSFT.O");
        let fetcher = MemoryFetcher {
            responses: HashMap::from([
                (apple.clone(), "fetched-apple".to_string()),
                (msft.clone(), "fetched-msft".to_string()),
            ]),
            fetch_count: Cell::new(0),
            omit_last: true,
        };

        let map = AwaitableMap::new(MemoryPersistence::default(), fetcher);
        let error = map
            .query(&[apple, msft])
            .await
            .expect_err("query should fail when one key is still missing");

        assert!(matches!(error, AwaitableMapError::MissingFetchedKeys));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn daily_map_persists_across_instances_for_the_same_date() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let tempdir = tempdir().expect("tempdir should exist");

        let first_fetcher = MemoryFetcher {
            responses: HashMap::from([(apple.clone(), "fetched-apple".to_string())]),
            fetch_count: Cell::new(0),
            omit_last: false,
        };
        let first =
            DailyAwaitableMap::with_root_directory("20260602", tempdir.path(), first_fetcher)
                .expect("daily map should be created");
        let first_result = first
            .query(std::slice::from_ref(&apple))
            .await
            .expect("first query should fetch and persist")
            .values;
        assert_eq!(first_result, vec!["fetched-apple".to_string()]);
        assert_eq!(first.fetcher.fetch_count.get(), 1);
        drop(first);

        let second_fetcher = MemoryFetcher {
            responses: HashMap::new(),
            fetch_count: Cell::new(0),
            omit_last: false,
        };
        let second =
            DailyAwaitableMap::with_root_directory("20260602", tempdir.path(), second_fetcher)
                .expect("second daily map should be created");
        let second_result = second
            .query(std::slice::from_ref(&apple))
            .await
            .expect("second query should read from persistence")
            .values;

        assert_eq!(second_result, vec!["fetched-apple".to_string()]);
        assert_eq!(second.fetcher.fetch_count.get(), 0);
        assert_eq!(second.date(), "20260602");
        assert_eq!(
            second.database_path(),
            Some(
                tempdir
                    .path()
                    .join("ds")
                    .join("20260602")
                    .join("20260602.sqlite")
                    .as_path()
            )
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delete_many_evicts_persisted_keys_and_forces_refetch() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let msft = InstrumentDuplet::new("Ric", "MSFT.O");
        let tempdir = tempdir().expect("tempdir should exist");
        let fetcher = MemoryFetcher {
            responses: HashMap::from([
                (apple.clone(), "fetched-apple".to_string()),
                (msft.clone(), "fetched-msft".to_string()),
            ]),
            fetch_count: Cell::new(0),
            omit_last: false,
        };
        let map = DailyAwaitableMap::with_root_directory("20260602", tempdir.path(), fetcher)
            .expect("daily map should be created");
        let keys = [apple.clone(), msft.clone()];

        map.query(&keys).await.expect("initial query persists");
        assert_eq!(map.count_cached(&keys).expect("count"), 2);
        assert_eq!(map.fetcher.fetch_count.get(), 1);

        // Evict apple; only msft remains cached.
        map.delete_many(std::slice::from_ref(&apple))
            .expect("delete should succeed");
        assert_eq!(map.count_cached(&keys).expect("count"), 1);

        // Re-querying re-fetches only the evicted key.
        map.query(&keys).await.expect("second query re-fetches apple");
        assert_eq!(map.fetcher.fetch_count.get(), 2);
        assert_eq!(map.count_cached(&keys).expect("count"), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn daily_map_in_memory_does_not_persist_across_instances() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");

        let first = DailyAwaitableMap::with_retry_state_in_memory(
            "20260602",
            MemoryFetcher {
                responses: HashMap::from([(apple.clone(), "fetched-apple".to_string())]),
                fetch_count: Cell::new(0),
                omit_last: false,
            },
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Duration::from_secs(30),
        )
        .expect("daily map should be created");
        let first_result = first
            .query(std::slice::from_ref(&apple))
            .await
            .expect("first query should fetch and store in memory")
            .values;
        assert_eq!(first_result, vec!["fetched-apple".to_string()]);
        assert_eq!(first.fetcher.fetch_count.get(), 1);
        assert_eq!(first.database_path(), None);

        let second = DailyAwaitableMap::with_retry_state_in_memory(
            "20260602",
            MemoryFetcher {
                responses: HashMap::new(),
                fetch_count: Cell::new(0),
                omit_last: false,
            },
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Duration::from_secs(30),
        )
        .expect("second daily map should be created");
        let error = match second.query(std::slice::from_ref(&apple)).await {
            Ok(_) => panic!("second query should not see entries from the first in-memory map"),
            Err(error) => error,
        };

        assert!(matches!(error, AwaitableMapError::MissingFetchedKeys));
        assert_eq!(second.fetcher.fetch_count.get(), 1);
        assert_eq!(second.database_path(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_with_progress_reports_started_extractions() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let tempdir = tempdir().expect("tempdir should exist");
        let started_ids = Arc::new(Mutex::new(Vec::new()));
        let fetcher = ProgressMemoryFetcher {
            responses: HashMap::from([(apple.clone(), "fetched-apple".to_string())]),
            started_ids: started_ids.clone(),
        };

        let map = DailyAwaitableMap::with_root_directory("20260602", tempdir.path(), fetcher)
            .expect("daily map should be created");
        let mut progress = Vec::new();
        let result = map
            .query_with_progress(std::slice::from_ref(&apple), |extraction_id| {
                progress.push(extraction_id);
            })
            .await
            .expect("query should succeed")
            .values;

        assert_eq!(result, vec!["fetched-apple".to_string()]);
        assert_eq!(progress, vec![1]);
        assert_eq!(*started_ids.lock().unwrap(), vec![1]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_with_observed_progress_reports_awaited_extractions() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let tempdir = tempdir().expect("tempdir should exist");
        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert(apple.clone(), PendingEntry::new(42));
        let failures = Arc::new(Mutex::new(HashMap::new()));
        let map = DailyAwaitableMap::with_database_path_and_retry_state(
            "20260602",
            tempdir.path().join("20260602.sqlite"),
            ProgressMemoryFetcher {
                responses: HashMap::new(),
                started_ids: Arc::new(Mutex::new(Vec::new())),
            },
            pending.clone(),
            failures,
            Duration::from_secs(30),
        )
        .expect("daily map should be created");

        let pending_for_task = pending.clone();
        let apple_for_task = apple.clone();
        let resolver = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            pending_for_task
                .lock()
                .unwrap()
                .get(&apple_for_task)
                .expect("pending entry should exist")
                .resolve(Arc::new("awaited-apple".to_string()));
        });

        let mut progress = Vec::new();
        let result = map
            .query_with_observed_progress(std::slice::from_ref(&apple), |event| {
                progress.push(event);
            })
            .await
            .expect("query should succeed")
            .values;
        resolver.await.expect("resolver task should complete");

        assert_eq!(result, vec!["awaited-apple".to_string()]);
        assert_eq!(
            progress,
            vec![QueryProgress::AwaitingExtractions {
                extraction_ids: vec![42]
            }]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_with_observed_progress_overlaps_waiting_and_fetching() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let msft = InstrumentDuplet::new("Ric", "MSFT.O");
        let tempdir = tempdir().expect("tempdir should exist");
        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert(apple.clone(), PendingEntry::new(42));
        let failures = Arc::new(Mutex::new(HashMap::new()));
        let (fetch_started_tx, fetch_started_rx) = tokio::sync::oneshot::channel();
        let release_fetch = Arc::new(tokio::sync::Notify::new());
        let map = DailyAwaitableMap::with_database_path_and_retry_state(
            "20260602",
            tempdir.path().join("20260602.sqlite"),
            CoordinatedProgressMemoryFetcher {
                responses: HashMap::from([(msft.clone(), "fetched-msft".to_string())]),
                fetch_started: Arc::new(Mutex::new(Some(fetch_started_tx))),
                release_fetch: release_fetch.clone(),
            },
            pending.clone(),
            failures,
            Duration::from_secs(30),
        )
        .expect("daily map should be created");

        let pending_for_task = pending.clone();
        let apple_for_task = apple.clone();
        let resolver = tokio::spawn(async move {
            fetch_started_rx
                .await
                .expect("fetch should start before awaited extraction resolves");
            let mut pending_guard = pending_for_task.lock().unwrap();
            pending_guard
                .get(&apple_for_task)
                .expect("pending entry should exist")
                .resolve(Arc::new("awaited-apple".to_string()));
            pending_guard.remove(&apple_for_task);
            release_fetch.notify_one();
        });

        let mut progress = Vec::new();
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            map.query_with_observed_progress(&[apple.clone(), msft.clone()], |event| {
                progress.push(event);
            }),
        )
        .await
        .expect("query should not deadlock")
        .expect("query should succeed");
        resolver.await.expect("resolver task should complete");

        assert_eq!(
            result.values,
            vec!["awaited-apple".to_string(), "fetched-msft".to_string()]
        );
        assert_eq!(
            progress,
            vec![
                QueryProgress::AwaitingExtractions {
                    extraction_ids: vec![42]
                },
                QueryProgress::Fetch(7)
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_with_progress_applies_failure_cooldown_before_refetching() {
        let apple = InstrumentDuplet::new("Ric", "AAPL.O");
        let tempdir = tempdir().expect("tempdir should exist");
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let failures = Arc::new(Mutex::new(HashMap::new()));
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let map = DailyAwaitableMap::with_database_path_and_retry_state(
            "20260602",
            tempdir.path().join("20260602.sqlite"),
            FailingProgressMemoryFetcher {
                fetch_count: fetch_count.clone(),
                failures: failures.clone(),
                failure_cooldown: Duration::from_secs(30),
            },
            pending,
            failures,
            Duration::from_secs(30),
        )
        .expect("daily map should be created");

        let first = match map
            .query_with_progress(std::slice::from_ref(&apple), |_| {})
            .await
        {
            Ok(_) => panic!("first query should fail through the fetcher"),
            Err(error) => error,
        };
        assert!(matches!(first, AwaitableMapError::Fetch("boom")));

        let second = match map
            .query_with_progress(std::slice::from_ref(&apple), |_| {})
            .await
        {
            Ok(_) => panic!("second query should respect the shared cooldown"),
            Err(error) => error,
        };
        assert!(matches!(second, AwaitableMapError::FetchCooldown { .. }));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn daily_map_rejects_invalid_dates() {
        let result =
            DailyAwaitableMap::<InstrumentDuplet, String, MemoryFetcher>::with_root_directory(
                "2026-06-02",
                ".",
                MemoryFetcher {
                    responses: HashMap::new(),
                    fetch_count: Cell::new(0),
                    omit_last: false,
                },
            );
        assert!(matches!(result, Err(SqliteMapError::InvalidDate(_))));
    }
}
