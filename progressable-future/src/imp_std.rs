use core::convert::Infallible;
use core::future::Future;

use futures::stream::{self, Stream, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex};

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

/// Steps emitted by the future
#[derive(Debug, Clone)]
pub enum Step<P, T, E, X = Infallible> {
    ///Progress
    Progress(P),
    ///Success
    Success(T),
    ///Error
    Error(E),
    ///Transport-side issue
    Transport(TransportIssue<X>),
}

/// Sender for progress events
#[derive(Clone)]
pub struct ProgressSender<P> {
    tx: mpsc::UnboundedSender<P>,
}

impl<P> ProgressSender<P> {
    ///Send the Progress
    pub fn progress(&self, value: P) {
        let _ = self.tx.send(value);
    }
}

/// A Future that can emit progress updates
pub struct ProgressableFuture<P, T, E, X = Infallible> {
    progress_rx: Mutex<Option<mpsc::UnboundedReceiver<P>>>,
    result_rx: Mutex<Option<oneshot::Receiver<ProgressOutcome<T, E, X>>>>,
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

impl<P: Send + 'static, T: Send + 'static, E: Send + 'static>
    ProgressableFuture<P, T, E, core::convert::Infallible>
{
    /// Create a new `ProgressableFuture`.
    ///
    /// Progress and the result/error types are fully caller-defined.
    /// The `X` transport-error type defaults to `Infallible`; use
    /// [`new_with_transport`] if you need a concrete transport-error type.
    pub fn new<Fut, F>(operation: F) -> Self
    where
        F: FnOnce(ProgressSender<P>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        ProgressableFuture::<P, T, E, core::convert::Infallible>::new_with_transport(operation)
    }
}

impl<P: Send + 'static, T: Send + 'static, E: Send + 'static, X: Send + 'static>
    ProgressableFuture<P, T, E, X>
{
    /// Create a new `ProgressableFuture` that may also surface transport failures via `X`.
    pub fn new_with_transport<Fut, F, O>(operation: F) -> Self
    where
        F: FnOnce(ProgressSender<P>) -> Fut + Send + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: Into<ProgressOutcome<T, E, X>> + Send + 'static,
    {
        let (progress_tx, progress_rx) = mpsc::unbounded_channel();
        let sender = ProgressSender { tx: progress_tx };
        let (result_tx, result_rx) = oneshot::channel();

        tokio::spawn(async move {
            let res = operation(sender).await.into();
            let _ = result_tx.send(res);
        });

        Self {
            progress_rx: Mutex::new(Some(progress_rx)),
            result_rx: Mutex::new(Some(result_rx)),
        }
    }

    /// Convert into a Stream of `Step<P, T, E>`
    pub fn stream(self) -> impl Stream<Item = Step<P, T, E, X>> {
        // Extract the receiver out of the Mutex up front — stream() consumes self,
        // so we can synchronously take ownership without holding the lock across yields.
        let progress_rx: Option<_> = self.progress_rx.into_inner();
        let progress_stream = stream::unfold(progress_rx, |mut rx_opt| async move {
            let rx = rx_opt.as_mut()?;
            rx.recv().await.map(|progress| (Step::Progress(progress), rx_opt))
        });

        let result_rx = self.result_rx;

        let result_stream = stream::once(async move {
            let rx = result_rx.lock().await.take();
            if let Some(rx) = rx {
                match rx.await {
                    Ok(ProgressOutcome::Success(val)) => Step::Success(val),
                    Ok(ProgressOutcome::Error(e)) => Step::Error(e),
                    Ok(ProgressOutcome::Transport(issue)) => Step::Transport(issue),
                    Err(_) => Step::Transport(TransportIssue::Interrupted),
                }
            } else {
                Step::Transport(TransportIssue::Interrupted)
            }
        });

        progress_stream.chain(result_stream)
    }

    /// Await the final caller-visible outcome (ignores progress).
    pub async fn await_result(self) -> ProgressOutcome<T, E, X> {
        let rx = self.result_rx.lock().await.take();
        if let Some(rx) = rx {
            match rx.await {
                Ok(outcome) => outcome,
                Err(_) => ProgressOutcome::Transport(TransportIssue::Interrupted),
            }
        } else {
            ProgressOutcome::Transport(TransportIssue::Interrupted)
        }
    }

}
