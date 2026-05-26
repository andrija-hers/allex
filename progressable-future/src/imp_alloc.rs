#![cfg(not(feature = "std"))]
extern crate alloc;

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use alloc::sync::Arc;
use core::marker::PhantomData;

/// A compact name: Step = Progress | Success | Error
#[derive(Debug, Clone)]
pub enum Step<P, T, E> {
    Progress(P),
    Success(T),
    Error(E),
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
pub struct ProgressableFuture<P, T, E> {
    progress_sender: Arc<dyn ProgressSend<P> + Send + Sync>,
    // the future part of this type is runtime-dependent: we represent it by a
    // boxed "waiter" future that will resolve to Result<T,E>.
    waiter: Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>,
    _phantom: PhantomData<(P, T, E)>,
}

impl<P, T, E> ProgressableFuture<P, T, E> {
    /// Construct the ProgressableFuture from user-supplied builder values:
    ///
    /// - `progress_sender` — an object that implements ProgressSend<P>.
    /// - `waiter` — a Future that resolves to Result<T,E> when the operation finishes.
    ///
    /// Important: in no_std you must supply the runtime glue: how the
    /// waiter is polled / woken. This API avoids taking a global dependency.
    pub fn from_parts<S, Fut>(progress_sender: S, waiter: Fut) -> Self
    where
        S: ProgressSend<P> + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        Self {
            progress_sender: Arc::new(progress_sender),
            waiter: Box::pin(waiter),
            _phantom: PhantomData,
        }
    }

    /// Emit progress. The ProgressSend impl decides how to deliver it.
    pub fn progress(&self, p: P) {
        self.progress_sender.send(p)
    }

    /// Convert to a stream of Step events — this is runtime-agnostic and returns
    /// a boxed stream that the user runtime can poll.
    pub fn into_stream(self) -> Pin<Box<dyn Future<Output = Step<P, T, E>> + Send>> {
        // Minimal placeholder: in no_std we cannot provide a blocking stream helper.
        // The user is expected to provide an adapter that polls `waiter` and uses their
        // progress sink to observe progress.
        //
        // For portability we return a future that resolves to Success/Error once waiter completes.
        Box::pin(async move {
            match self.waiter.await {
                Ok(ok) => Step::Success(ok),
                Err(e) => Step::Error(e),
            }
        })
    }
}
