use std::sync::Arc;

use tokio::sync::{RwLock, watch};
use tokio_stream::{Stream, wrappers::WatchStream};

/// Represents a change that can be applied to a state.
pub trait StateUpdate: Clone + Send + Sync + 'static {
    type State;

    fn apply(&self, state: &mut Self::State);
}

/// Authoritative source of state.
///
/// Semantics:
/// - owns the canonical state
/// - emits latest-state notifications
/// - listeners may skip intermediate updates
pub trait StateSource: Send + Sync + 'static {
    type State: Clone + Send + Sync + 'static;
    type Update: StateUpdate<State = Self::State>;

    type Stream: Stream<Item = Self::State> + Send + 'static;

    fn current(&self) -> Self::State;

    fn subscribe(&self) -> Self::Stream;

    fn update(&self, update: Self::Update);
}

/// Local authoritative state implementation.
pub struct LocalState<S, U>
where
    S: Clone + Send + Sync + 'static,
    U: StateUpdate<State = S>,
{
    state: Arc<RwLock<S>>,
    tx: watch::Sender<S>,
    _phantom: std::marker::PhantomData<U>,
}

impl<S, U> LocalState<S, U>
where
    S: Clone + Send + Sync + 'static,
    U: StateUpdate<State = S>,
{
    pub fn new(initial: S) -> Self {
        let (tx, _) = watch::channel(initial.clone());

        Self {
            state: Arc::new(RwLock::new(initial)),
            tx,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, U> StateSource for LocalState<S, U>
where
    S: Clone + Send + Sync + 'static,
    U: StateUpdate<State = S>,
{
    type State = S;
    type Update = U;
    type Stream = WatchStream<S>;

    fn current(&self) -> Self::State {
        self.tx.borrow().clone()
    }

    fn subscribe(&self) -> Self::Stream {
        WatchStream::new(self.tx.subscribe())
    }

    fn update(&self, update: Self::Update) {
        let mut guard = self.state.blocking_write();

        update.apply(&mut *guard);

        let _ = self.tx.send(guard.clone());
    }
}

/// Read-only mirrored state.
///
/// RemoteState reflects another StateSource.
/// It does not own authoritative state.
pub struct RemoteState<S>
where
    S: Clone + Send + Sync + 'static,
{
    state: Arc<RwLock<S>>,
}

impl<S> RemoteState<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Start mirroring another source.
    pub fn new<Src>(source: &Src) -> Self
    where
        Src: StateSource<State = S>,
    {
        let initial = source.current();

        let state = Arc::new(RwLock::new(initial));

        let mut stream = Box::pin(source.subscribe());

        let mirror = state.clone();

        tokio::spawn(async move {
            use tokio_stream::StreamExt;

            while let Some(next) = stream.next().await {
                let mut guard = mirror.write().await;
                *guard = next;
            }
        });

        Self { state }
    }

    /// Read current mirrored snapshot.
    pub async fn current(&self) -> S {
        self.state.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct CounterState {
        value: i32,
    }

    #[derive(Clone, Debug)]
    enum CounterUpdate {
        Increment,
        Decrement,
        Add(i32),
    }

    impl StateUpdate for CounterUpdate {
        type State = CounterState;

        fn apply(&self, state: &mut Self::State) {
            match self {
                Self::Increment => state.value += 1,
                Self::Decrement => state.value -= 1,
                Self::Add(v) => state.value += *v,
            }
        }
    }

    #[tokio::test]
    async fn remote_state_reflects_local_state() {
        let local = LocalState::<CounterState, CounterUpdate>::new(CounterState { value: 0 });

        let remote = RemoteState::new(&local);

        local.update(CounterUpdate::Increment);
        local.update(CounterUpdate::Add(10));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let current = remote.current().await;

        assert_eq!(current, CounterState { value: 11 });
    }

    #[tokio::test]
    async fn subscribers_receive_updates() {
        use tokio_stream::StreamExt;

        let local = LocalState::<CounterState, CounterUpdate>::new(CounterState { value: 0 });

        let mut stream = Box::pin(local.subscribe());

        let initial = stream.next().await.unwrap();

        assert_eq!(initial, CounterState { value: 0 });

        local.update(CounterUpdate::Increment);

        let updated = stream.next().await.unwrap();

        assert_eq!(updated, CounterState { value: 1 });
    }

    #[tokio::test]
    async fn decrement_update_works() {
        let local = LocalState::<CounterState, CounterUpdate>::new(CounterState { value: 10 });

        local.update(CounterUpdate::Decrement);

        assert_eq!(local.current(), CounterState { value: 9 });
    }
}
