//! Generic path-addressed state with change-suppressing mutations.
//!
//! # Core concepts
//!
//! - [`HasUpdate`] — implemented by any struct that derives [`State`]; links
//!   a state type to its generated `Update` enum and `apply_update`.
//! - [`LocalState<S>`] — authoritative, in-process state store.  Clonable
//!   handle; all clones share the same underlying value.  Broadcasts the full
//!   new state to subscribers on every *non-suppressed* mutation.
//! - [`StateHandle<S>`] — for concurrent tasks that own a sub-tree.  Wraps a
//!   `LocalState<Root>` together with a path-lifting closure so the task can
//!   call `handle.apply(leaf_update)` and have it arrive at the root correctly.
//! - [`RemoteState<S>`] — read-only mirror of a `LocalState` living in another
//!   task; auto-syncs via a background task.
//!
//! # Derive macro
//!
//! Use `#[derive(State)]` (from the companion `allex-state-macro` crate) to
//! generate the `HasUpdate` impl and mutation helpers for your structs.

pub use allex_state_macro::State;
pub use allex_state_macro::state_type;

use progressable_future::{ProgressSender, ProgressableFuture};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tokio_stream::{Stream, wrappers::WatchStream};

// ---------------------------------------------------------------------------
// HasUpdate — the core trait implemented by #[derive(State)]
// ---------------------------------------------------------------------------

/// Implemented by structs that derive [`State`].
///
/// Links a state type to its generated `XxxUpdate` enum and provides the
/// single `apply_update` dispatch that the infrastructure uses to mutate state.
pub trait HasUpdate: Clone + Send + Sync + 'static {
    type Update: Clone + Send + Sync + 'static;

    /// Apply a single update in-place.  Called inside `send_modify` so the
    /// mutation and the broadcast are atomic from the watcher's point of view.
    fn apply_update(&mut self, update: Self::Update);
}

// ---------------------------------------------------------------------------
// LocalState<S> — authoritative in-process store
// ---------------------------------------------------------------------------

/// Authoritative, clonable handle to a piece of state.
///
/// All clones share the same underlying `watch::Sender`.  Mutations flow
/// through [`apply`] which calls `send_modify`; the broadcast only fires when
/// the closure actually changes the value (i.e. when the `Option<Update>` is
/// `Some`).
#[derive(Clone)]
pub struct LocalState<S: HasUpdate> {
    tx: Arc<watch::Sender<S>>,
}

impl<S: HasUpdate> LocalState<S> {
    pub fn new(initial: S) -> Self {
        let (tx, _) = watch::channel(initial);
        Self { tx: Arc::new(tx) }
    }

    /// Apply an optional update.  When `update` is `None` (suppressed by
    /// change-detection in the generated setter) this is a true no-op —
    /// no lock, no broadcast.
    pub fn apply(&self, update: Option<S::Update>) {
        if let Some(u) = update {
            self.tx.send_modify(|state| state.apply_update(u));
        }
    }

    /// Replace the entire state with a new value, always broadcasting.
    ///
    /// Use this for bulk rebuilds (structural vec changes, initial publish).
    /// For targeted field-level changes prefer [`apply`] so unchanged-value
    /// suppression works.
    pub fn replace(&self, new_state: S) {
        self.tx.send_modify(|state| *state = new_state);
    }

    /// Mutate via a closure that returns `Option<Update>`.  Useful for
    /// compound mutations where you want to read and write in one step.
    pub fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut S) -> Option<S::Update>,
    {
        self.tx.send_modify(|state| {
            if let Some(u) = f(state) {
                // Update already applied by the closure; nothing more to do.
                // We need apply_update only if the closure returned the update
                // WITHOUT applying it itself.  Convention: the closure applies
                // the change AND returns the update for upstream propagation.
                // So here we just let send_modify see the already-mutated state.
                let _ = u; // update was already applied inside the closure
            }
        });
    }

    /// Returns a clone of the current state.
    pub fn current(&self) -> S {
        self.tx.borrow().clone()
    }

    /// Subscribe to state changes.  The stream yields the full new state on
    /// every non-suppressed mutation.
    pub fn subscribe(&self) -> impl Stream<Item = S> + Send + 'static {
        WatchStream::new(self.tx.subscribe())
    }

    /// Returns true if at least one subscriber is currently connected.
    pub fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// Returns a raw [`watch::Receiver`] for callers that need
    /// `changed()` / `borrow_and_update()` semantics.
    pub fn subscribe_receiver(&self) -> watch::Receiver<S> {
        self.tx.subscribe()
    }

    /// Stream state updates as a [`ProgressableFuture`] that never terminates
    /// normally (`T = Infallible`).  Each state change is mapped to a progress
    /// value `P` via `map`.  The stream errors with `E` only if the watch
    /// channel closes (i.e. the owning task dropped `LocalState`).
    ///
    /// Encapsulates the `current()` + `subscribe_receiver()` + flush-race-guard
    /// pattern so every "billboard" RPC endpoint is a one-liner.
    ///
    /// # Example
    /// ```ignore
    /// state.watch_progressable(|snap| build_response(snap), || StateClosedError)
    /// ```
    pub fn watch_progressable<P, T, E, FM, FE>(
        &self,
        map: FM,
        on_closed: FE,
    ) -> ProgressableFuture<P, T, E>
    where
        P: Send + 'static,
        T: From<std::convert::Infallible> + Send + 'static,
        E: Send + 'static,
        FM: Fn(S) -> P + Send + 'static,
        FE: FnOnce() -> E + Send + 'static,
    {
        // Subscribe before reading current so no update is missed.
        // borrow_and_update() both returns the current value and marks it as
        // "seen", so the first changed() only fires on a genuine new change.
        let mut receiver = self.tx.subscribe();
        ProgressableFuture::new(move |sender: ProgressSender<P>| async move {
            let initial = receiver.borrow_and_update().clone();
            sender.progress(map(initial));
            loop {
                if receiver.changed().await.is_err() {
                    return Err(on_closed());
                }
                let snap = receiver.borrow_and_update().clone();
                sender.progress(map(snap));
            }
        })
    }
}

// ---------------------------------------------------------------------------
// StateHandle<Root, LeafUpdate> — for concurrent tasks owning a sub-tree
// ---------------------------------------------------------------------------

/// A handle given to a concurrent task that owns a sub-tree of state.
///
/// `Root` is the top-level state type stored in `LocalState`.
/// `LeafUpdate` is the update type for the sub-tree the task manipulates.
///
/// The handle wraps a `LocalState<Root>` plus a *lifting* closure that
/// converts a `LeafUpdate` into a `Root::Update`.  The task calls
/// `handle.apply(leaf_update)` without needing to know about the root type.
#[derive(Clone)]
pub struct StateHandle<Root, LeafUpdate>
where
    Root: HasUpdate,
    LeafUpdate: Clone + Send + Sync + 'static,
{
    state: LocalState<Root>,
    lift: Arc<dyn Fn(LeafUpdate) -> Root::Update + Send + Sync + 'static>,
}

impl<Root, LeafUpdate> StateHandle<Root, LeafUpdate>
where
    Root: HasUpdate,
    LeafUpdate: Clone + Send + Sync + 'static,
{
    pub fn new<F>(state: LocalState<Root>, lift: F) -> Self
    where
        F: Fn(LeafUpdate) -> Root::Update + Send + Sync + 'static,
    {
        Self {
            state,
            lift: Arc::new(lift),
        }
    }

    /// Apply a leaf-level optional update, lifting it to the root.
    /// No-op when `update` is `None`.
    pub fn apply(&self, update: Option<LeafUpdate>) {
        if let Some(u) = update {
            self.state.apply(Some((self.lift)(u)));
        }
    }

    /// Compose with a deeper lifting function to create a handle for an even
    /// lower level of the tree.
    pub fn nested<Deeper, F>(&self, lift_deeper: F) -> StateHandle<Root, Deeper>
    where
        Deeper: Clone + Send + Sync + 'static,
        F: Fn(Deeper) -> LeafUpdate + Send + Sync + 'static,
    {
        let outer_lift = Arc::clone(&self.lift);
        StateHandle {
            state: self.state.clone(),
            lift: Arc::new(move |deep| outer_lift(lift_deeper(deep))),
        }
    }
}

// ---------------------------------------------------------------------------
// RemoteState<S> — read-only mirror
// ---------------------------------------------------------------------------

/// A read-only mirror of a [`LocalState`] that lives in another task.
///
/// Spawns a background task to stay in sync via the watch stream.
pub struct RemoteState<S: HasUpdate> {
    inner: Arc<RwLock<S>>,
}

impl<S: HasUpdate> RemoteState<S> {
    pub async fn new(source: &LocalState<S>) -> Self {
        let initial = source.current();
        let inner = Arc::new(RwLock::new(initial));
        let mirror = Arc::clone(&inner);
        let mut stream = Box::pin(source.subscribe());
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            while let Some(next) = stream.next().await {
                *mirror.write().await = next;
            }
        });
        Self { inner }
    }

    pub async fn current(&self) -> S {
        self.inner.read().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal hand-written HasUpdate impl to test infrastructure
    // without depending on the derive macro here.

    #[derive(Clone, Debug, PartialEq)]
    struct Counter {
        value: i32,
        label: String,
    }

    #[derive(Clone, Debug)]
    enum CounterUpdate {
        ValueChanged(i32),
        LabelChanged(String),
    }

    impl HasUpdate for Counter {
        type Update = CounterUpdate;
        fn apply_update(&mut self, update: CounterUpdate) {
            match update {
                CounterUpdate::ValueChanged(v) => self.value = v,
                CounterUpdate::LabelChanged(s) => self.label = s,
            }
        }
    }

    impl Counter {
        fn set_value(&mut self, v: i32) -> Option<CounterUpdate> {
            if self.value == v {
                return None;
            }
            self.value = v;
            Some(CounterUpdate::ValueChanged(v))
        }
        fn set_label(&mut self, s: String) -> Option<CounterUpdate> {
            if self.label == s {
                return None;
            }
            self.label = s.clone();
            Some(CounterUpdate::LabelChanged(s))
        }
    }

    #[tokio::test]
    async fn apply_none_does_not_broadcast() {
        let state = LocalState::new(Counter {
            value: 0,
            label: "x".into(),
        });
        let mut rx = state.tx.subscribe();
        // consume the initial "changed" marker
        let _ = rx.borrow_and_update();

        state.apply(None);

        assert!(!rx.has_changed().unwrap(), "None update must not broadcast");
    }

    #[tokio::test]
    async fn apply_some_broadcasts() {
        let state = LocalState::new(Counter {
            value: 0,
            label: "x".into(),
        });
        let mut rx = state.tx.subscribe();
        let _ = rx.borrow_and_update();

        let update = {
            let mut c = state.current();
            c.set_value(42)
        };
        state.apply(update);

        assert!(rx.has_changed().unwrap(), "Some update must broadcast");
        assert_eq!(state.current().value, 42);
    }

    #[tokio::test]
    async fn set_suppresses_duplicate() {
        let state = LocalState::new(Counter {
            value: 5,
            label: "x".into(),
        });
        let mut rx = state.tx.subscribe();
        let _ = rx.borrow_and_update();

        // set same value — should be suppressed
        let update = {
            let mut c = state.current();
            c.set_value(5)
        };
        state.apply(update);

        assert!(!rx.has_changed().unwrap(), "equal set must not broadcast");
    }

    #[tokio::test]
    async fn state_handle_lifts_update() {
        #[derive(Clone, Debug, PartialEq)]
        struct Root {
            counter: Counter,
        }
        #[derive(Clone, Debug)]
        enum RootUpdate {
            Counter(CounterUpdate),
        }
        impl HasUpdate for Root {
            type Update = RootUpdate;
            fn apply_update(&mut self, update: RootUpdate) {
                match update {
                    RootUpdate::Counter(u) => self.counter.apply_update(u),
                }
            }
        }

        let state = LocalState::new(Root {
            counter: Counter {
                value: 0,
                label: "y".into(),
            },
        });
        let handle = StateHandle::new(state.clone(), RootUpdate::Counter);

        let child_update = {
            let mut c = state.current();
            c.counter.set_value(99)
        };
        handle.apply(child_update);

        assert_eq!(state.current().counter.value, 99);
    }

    #[tokio::test]
    async fn remote_state_mirrors_local() {
        let local = LocalState::new(Counter {
            value: 0,
            label: "z".into(),
        });
        let remote = RemoteState::new(&local).await;

        let update = {
            let mut c = local.current();
            c.set_value(7)
        };
        local.apply(update);

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(remote.current().await.value, 7);
    }

    #[tokio::test]
    async fn nested_handle_composes() {
        #[derive(Clone, Debug, PartialEq)]
        struct Inner {
            x: i32,
        }
        #[derive(Clone, Debug)]
        enum InnerUpdate {
            XChanged(i32),
        }
        impl HasUpdate for Inner {
            type Update = InnerUpdate;
            fn apply_update(&mut self, u: InnerUpdate) {
                match u {
                    InnerUpdate::XChanged(v) => self.x = v,
                }
            }
        }
        impl Inner {
            fn set_x(&mut self, v: i32) -> Option<InnerUpdate> {
                if self.x == v {
                    return None;
                }
                self.x = v;
                Some(InnerUpdate::XChanged(v))
            }
        }

        #[derive(Clone, Debug, PartialEq)]
        struct Mid {
            inner: Inner,
        }
        #[derive(Clone, Debug)]
        enum MidUpdate {
            Inner(InnerUpdate),
        }
        impl HasUpdate for Mid {
            type Update = MidUpdate;
            fn apply_update(&mut self, u: MidUpdate) {
                match u {
                    MidUpdate::Inner(u) => self.inner.apply_update(u),
                }
            }
        }

        #[derive(Clone, Debug, PartialEq)]
        struct Root {
            mid: Mid,
        }
        #[derive(Clone, Debug)]
        enum RootUpdate {
            Mid(MidUpdate),
        }
        impl HasUpdate for Root {
            type Update = RootUpdate;
            fn apply_update(&mut self, u: RootUpdate) {
                match u {
                    RootUpdate::Mid(u) => self.mid.apply_update(u),
                }
            }
        }

        let state = LocalState::new(Root {
            mid: Mid {
                inner: Inner { x: 0 },
            },
        });
        let mid_handle = StateHandle::new(state.clone(), RootUpdate::Mid);
        let inner_handle = mid_handle.nested(MidUpdate::Inner);

        let update = {
            let mut s = state.current();
            s.mid.inner.set_x(55)
        };
        inner_handle.apply(update);

        assert_eq!(state.current().mid.inner.x, 55);
    }
}
