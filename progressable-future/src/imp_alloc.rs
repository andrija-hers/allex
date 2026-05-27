#![cfg(not(feature = "std"))]
extern crate alloc;

use alloc::boxed::Box;
use core::convert::Infallible;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use alloc::sync::Arc;
use core::marker::PhantomData;

/// Transport-side issues seen by the caller.
#[derive(Debug, Clone)]
pub enum TransportIssue<X = Infallible> {
    /// A concrete transport error.
    Error(X),
    /// The progressive exchange stopped before a terminal value arrived.
    Interrupted,
}

/// Final caller-visible outcome.
#[derive(Debug, Clone)]
pub enum ProgressOutcome<T, E, X = Infallible> {
    /// Operation completed successfully.
    Success(T),
    /// Operation completed with an application error.
    Error(E),
    /// Operation could not be completed because the progressive transport failed.
    Transport(TransportIssue<X>),
}

impl<T, E, X> From<Result<T, E>> for ProgressOutcome<T, E, X> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(result) => Self::Success(result),
            Err(error) => Self::Error(error),
        }
    }
}

/// A compact name: Step = Progress | Success | Error
#[derive(Debug, Clone)]
pub enum Step<P, T, E, X = Infallible> {
    Progress(P),
    Success(T),
    Error(E),
    Transport(TransportIssue<X>),
}

/// ProgressSender trait (runtime must provide an implementation).
pub trait ProgressSend<P> {
    fn send(&self, p: P);
}

/// ResultCompleter trait — used to complete the final result.
///
/// The runtime must provide an implementation that will cause `ProgressableFuture`
///'s internal completion to be observed by future/poll.
pub trait ResultCompleter<T, E> {
    /// complete final result; this should wake any tasks waiting.
    fn complete(self: Box<Self>, res: Result<T, E>);
}

/// A small handle that represents an externally-managed progressable.
/// You construct it with your runtime: provide a ProgressSend impl and a
/// ResultCompleter that will be called when the operation completes.
pub struct ProgressableFuture<P, T, E, X = Infallible> {
    progress_sender: Arc<dyn ProgressSend<P> + Send + Sync>,
    // the future part of this type is runtime-dependent: we represent it by a
    // boxed "waiter" future that will resolve to the final caller-visible outcome.
    waiter: Pin<Box<dyn Future<Output = ProgressOutcome<T, E, X>> + Send + 'static>>,
    _phantom: PhantomData<(P, T, E, X)>,
}

impl<P, T, E, X> ProgressableFuture<P, T, E, X> {
    /// Construct the ProgressableFuture from user-supplied builder values:
    ///
    /// - `progress_sender` — an object that implements ProgressSend<P>.
    /// - `waiter` — a Future that resolves to Result<T,E> when the operation finishes.
    ///
    /// Important: in no_std you must supply the runtime glue: how the
    /// waiter is polled / woken. This API avoids taking a global dependency.
    pub fn from_parts<S, Fut, O>(progress_sender: S, waiter: Fut) -> Self
    where
        S: ProgressSend<P> + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: Into<ProgressOutcome<T, E, X>> + 'static,
    {
        Self {
            progress_sender: Arc::new(progress_sender),
            waiter: Box::pin(async move { waiter.await.into() }),
            _phantom: PhantomData,
        }
    }

    /// Emit progress. The ProgressSend impl decides how to deliver it.
    pub fn progress(&self, p: P) {
        self.progress_sender.send(p)
    }

    /// Convert to a stream of Step events — this is runtime-agnostic and returns
    /// a boxed stream that the user runtime can poll.
    pub fn into_stream(self) -> Pin<Box<dyn Future<Output = Step<P, T, E, X>> + Send>> {
        // Minimal placeholder: in no_std we cannot provide a blocking stream helper.
        // The user is expected to provide an adapter that polls `waiter` and uses their
        // progress sink to observe progress.
        //
        // For portability we return a future that resolves to Success/Error once waiter completes.
        Box::pin(async move {
            match self.waiter.await {
                ProgressOutcome::Success(ok) => Step::Success(ok),
                ProgressOutcome::Error(e) => Step::Error(e),
                ProgressOutcome::Transport(issue) => Step::Transport(issue),
            }
        })
    }

    /// Await the final caller-visible outcome.
    pub async fn await_result(self) -> ProgressOutcome<T, E, X> {
        self.waiter.await
    }
}

impl<T, E, X> ProgressOutcome<T, E, X> {
    /// Flatten the outcome into a `Result`, folding a transport issue into `E`
    /// via `From`. Useful for bubbling all failures up with `?`.
    pub fn flatten(self) -> Result<T, E>
    where
        E: From<TransportIssue<X>>,
    {
        match self {
            ProgressOutcome::Success(value) => Ok(value),
            ProgressOutcome::Error(error) => Err(error),
            ProgressOutcome::Transport(issue) => Err(E::from(issue)),
        }
    }
}
