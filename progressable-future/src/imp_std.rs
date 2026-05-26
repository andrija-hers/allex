use tokio::sync::{broadcast, oneshot, Mutex};
use futures::stream::{self, Stream, StreamExt};

/// Steps emitted by the future
#[derive(Debug, Clone)]
pub enum Step<P, T, E> {
    ///Progress
    Progress(P),
    ///Success
    Success(T),
    ///Error
    Error(E),
    ///MissedSome
    MissedSome,
    ///Canceled
    Canceled,
    ///Dropped
    Dropped,
}

/// Sender for progress events
#[derive(Clone)]
pub struct ProgressSender<P> {
    tx: broadcast::Sender<P>,
}

impl<P: Clone> ProgressSender<P> {
    ///Send the Progress
    pub fn progress(&self, value: P) {
        let _ = self.tx.send(value);
    }
}

/// A Future that can emit progress updates
pub struct ProgressableFuture<P, T, E> {
    progress_tx: broadcast::Sender<P>,
    result_rx: Mutex<Option<oneshot::Receiver<Result<T, E>>>>,
}

impl<P: Clone + Send + 'static, T: Send + 'static, E: Send + 'static> ProgressableFuture<P, T, E> {
    /// Create a new `ProgressableFuture`
    pub fn new<Fut, F>(operation: F) -> Self
    where
        F: FnOnce(ProgressSender<P>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
    {
        let (progress_tx, _) = broadcast::channel(32);
        let sender = ProgressSender { tx: progress_tx.clone() };
        let (result_tx, result_rx) = oneshot::channel();

        tokio::spawn(async move {
            let res = operation(sender).await;
            let _ = result_tx.send(res);
        });

        Self {
            progress_tx,
            result_rx: Mutex::new(Some(result_rx)),
        }
    }

    /// Convert into a Stream of `Step<P, T, E>`
    pub fn stream(self) -> impl Stream<Item = Step<P, T, E>> {
        let progress_rx = self.progress_tx.subscribe();
        let progress_stream = stream::unfold(progress_rx, |mut rx| async move {
            match rx.recv().await {
                Ok(p) => Some((Step::Progress(p), rx)),
                Err(broadcast::error::RecvError::Closed) => None,
                Err(broadcast::error::RecvError::Lagged(_)) => Some((Step::MissedSome, rx)), // optional
            }
        });

        let result_rx = self.result_rx;

        let result_stream = stream::once(async move {
            let rx = result_rx.lock().await.take();
            if let Some(rx) = rx {
                match rx.await {
                    Ok(Ok(val)) => Step::Success(val),
                    Ok(Err(e)) => Step::Error(e),
                    Err(_) => Step::Canceled,
                }
            } else {
                Step::Dropped
            }
        });

        progress_stream.chain(result_stream)
    }

    /// Await the final result (ignores progress)
    pub async fn await_result(self) -> Result<T, Option<E>> {
        let rx = self.result_rx.lock().await.take();
        if let Some(rx) = rx {
            match rx.await {
                Ok(Ok(val)) => Ok(val),
                Ok(Err(e)) => Err(Some(e)),
                Err(_) => Err(None), // canceled
            }
        } else {
            Err(None) // dropped
        }
    }
}
