use std::sync::{Arc, OnceLock};
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

/// Implement this on any struct that needs to react to changes of a watched
/// singleton value `C`.  Called once immediately on registration and again on
/// every subsequent change.
pub trait Apply<C> {
    fn apply(&mut self, value: &C);
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// Returned by [`WatchSingleton::register`]; drives value-change propagation.
pub struct Handle<C> {
    rx: watch::Receiver<Arc<C>>,
}

impl<C: Send + Sync + 'static> Handle<C> {
    /// Poll-based: apply the new value to `user` if it changed since last call.
    /// No-ops when nothing changed — safe to call on every tick.
    pub fn apply_if_changed<T: Apply<C>>(&mut self, user: &mut T) {
        if self.rx.has_changed().unwrap_or(false) {
            user.apply(&self.rx.borrow_and_update());
        }
    }

    /// Async: suspend until the next change, then apply it to `user`.
    pub async fn next_change<T: Apply<C>>(
        &mut self,
        user: &mut T,
    ) -> Result<(), watch::error::RecvError> {
        self.rx.changed().await?;
        user.apply(&self.rx.borrow_and_update());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AlreadyInitializedError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct AlreadyInitializedError;

impl std::fmt::Display for AlreadyInitializedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "singleton already initialized")
    }
}

impl std::error::Error for AlreadyInitializedError {}

// ---------------------------------------------------------------------------
// WatchSingleton
// ---------------------------------------------------------------------------

/// A generic `'static`-safe watched singleton.
///
/// # Typical usage
///
/// ```ignore
/// static CONFIG: WatchSingleton<MyConfig> = WatchSingleton::new();
///
/// // startup
/// CONFIG.init(value).unwrap();
///
/// // consumer
/// impl Apply<MyConfig> for MyStruct {
///     fn apply(&mut self, cfg: &MyConfig) { self.path = cfg.path.clone(); }
/// }
/// let handle = CONFIG.register(&mut my_struct); // applies immediately
///
/// // tick loop
/// handle.apply_if_changed(&mut my_struct);
/// ```
pub struct WatchSingleton<C> {
    tx: OnceLock<watch::Sender<Arc<C>>>,
}


impl<C: Send + Sync + 'static> WatchSingleton<C> {
    pub const fn new() -> Self {
        Self { tx: OnceLock::new() }
    }

    /// Initialize the singleton. Must be called once before any other method.
    pub fn init(&self, value: C) -> Result<(), AlreadyInitializedError> {
        let (tx, _rx) = watch::channel(Arc::new(value));
        self.tx.set(tx).map_err(|_| AlreadyInitializedError)
    }

    /// Cheap snapshot of the current value (increments `Arc` refcount).
    ///
    /// # Panics
    /// Panics if [`init`](Self::init) has not been called.
    pub fn current(&self) -> Arc<C> {
        Arc::clone(&self.sender().borrow())
    }

    /// Replace the current value and notify all subscribers.
    ///
    /// # Panics
    /// Panics if [`init`](Self::init) has not been called.
    pub fn reload(&self, value: C) {
        self.sender().send(Arc::new(value)).ok();
    }

    /// Low-level: subscribe to raw change notifications.
    /// Prefer [`register`](Self::register) for the idiomatic pattern.
    ///
    /// # Panics
    /// Panics if [`init`](Self::init) has not been called.
    pub fn subscribe(&self) -> watch::Receiver<Arc<C>> {
        self.sender().subscribe()
    }

    /// Register `user` as a consumer:
    /// 1. Calls `user.apply(current_value)` immediately.
    /// 2. Returns a [`Handle`] for driving future changes.
    ///
    /// # Panics
    /// Panics if [`init`](Self::init) has not been called.
    pub fn register<T: Apply<C>>(&self, user: &mut T) -> Handle<C> {
        let mut rx = self.sender().subscribe();
        user.apply(&rx.borrow_and_update());
        Handle { rx }
    }

    fn sender(&self) -> &watch::Sender<Arc<C>> {
        self.tx.get().expect("WatchSingleton not initialized")
    }
}
