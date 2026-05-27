use std::{future::Future, pin::Pin};

use futures::{Stream, StreamExt};
use progressable_future::{ProgressableFuture, Step};
use transport::{CallResponse, ClientSideChunkedTransport, Encodable, Error, Payload, ProgressOutcome, TransportIssue, UnaryCodec};
use crate::{AddRequest, AddResult, ArithmeticError, CountRequest, CountStep, MulRequest, MulResult, StepsMade};

pub trait ArithmeticV1Client<C: UnaryCodec> {
    fn add(&self, first: i32, second: i32) -> impl Future<Output = ProgressOutcome<AddResult, ArithmeticError, Error>> + Send;
    fn mul(&self, first: i32, second: i32) -> impl Future<Output = ProgressOutcome<MulResult, ArithmeticError, Error>> + Send;
    fn count(&self, from: i32, to: i32) -> impl Future<Output = Result<ProgressableFuture<CountStep, StepsMade, ArithmeticError, Error>, Error>> + Send;
}


use transport::{ClientSideOneShotTransport};

impl<T, C> ArithmeticV1Client<C> for T
where
    T: ClientSideOneShotTransport<C, AddRequest> + Sync
        + ClientSideOneShotTransport<C, MulRequest>
        + ClientSideChunkedTransport<C, CountRequest>,
    C: UnaryCodec + Send + Sync + 'static, {
    async fn add(&self, first: i32, second: i32) -> ProgressOutcome<AddResult, ArithmeticError, Error> {
        decode_one_shot_outcome::<C, AddResult, ArithmeticError>(
            <T as ClientSideOneShotTransport<C, AddRequest>>::execute_module_method_call(
                self,
                "arithmetic_v1",
                "add",
                AddRequest { a: first, b: second },
            )
            .await,
        )
    }
    async fn mul(&self, first: i32, second: i32) -> ProgressOutcome<MulResult, ArithmeticError, Error> {
        decode_one_shot_outcome::<C, MulResult, ArithmeticError>(
            <T as ClientSideOneShotTransport<C, MulRequest>>::execute_module_method_call(
                self,
                "arithmetic_v1",
                "mul",
                MulRequest { a: first, b: second },
            )
            .await,
        )
    }

    async fn count(&self, from: i32, to: i32) -> Result<ProgressableFuture<CountStep, StepsMade, ArithmeticError, Error>, Error> {
        let response_stream =
            <T as ClientSideChunkedTransport<C, CountRequest>>::execute_module_method_call_stream(
                self,
                "arithmetic_v1",
                "count",
                CountRequest { from, to },
            )
            .await?;

        Ok(ProgressableFuture::new_with_transport(move |sender| async move {
            let mut response_stream = response_stream;

            while let Some(frame) = response_stream.next().await {
                match frame {
                    Err(error) => {
                        return ProgressOutcome::Transport(TransportIssue::Error(error));
                    }
                    Ok(CallResponse::Progress(payload)) => {
                        let progress = match payload.decode::<C, CountStep>() {
                            Ok(progress) => progress,
                            Err(error) => return ProgressOutcome::Transport(TransportIssue::Error(error)),
                        };
                        sender.progress(progress);
                    }
                    Ok(CallResponse::Success(payload)) => {
                        return match payload.decode::<C, StepsMade>() {
                            Ok(result) => ProgressOutcome::Success(result),
                            Err(error) => ProgressOutcome::Transport(TransportIssue::Error(error)),
                        };
                    }
                    Ok(CallResponse::Error(payload)) => {
                        return match payload.decode::<C, ArithmeticError>() {
                            Ok(error) => ProgressOutcome::Error(error),
                            Err(error) => ProgressOutcome::Transport(TransportIssue::Error(error)),
                        };
                    }
                }
            }

            ProgressOutcome::Transport(TransportIssue::Interrupted)
        }))
    }
}

use async_trait::async_trait;
use transport::{Error as TransportError};
use execution::{MethodResponder, MethodResponderWithCodec, Response};

use crate::{ArithmeticV1};

pub struct ArithmeticV1MethodResponder<I, C> {
    inner: I,
    _codec: std::marker::PhantomData<C>,
}

impl<I, C> ArithmeticV1MethodResponder<I, C> {
    pub fn new(inner: I) -> Self {
        Self { inner, _codec: std::marker::PhantomData }
    }
}

pub type ArithmeticV1ResponseStream = Pin<Box<dyn Stream<Item = CallResponse> + Send>>;

impl<I, C> ArithmeticV1MethodResponder<I, C>
where
    I: ArithmeticV1 + Send + Sync,
    C: UnaryCodec + Send + Sync + 'static,
{
    pub fn execute_streaming(&self, method: &str, payload: Payload) -> Option<ArithmeticV1ResponseStream> {
        match method {
            "count" => {
                let req = match payload.decode::<C, CountRequest>() {
                    Ok(req) => req,
                    Err(err) => {
                        let error_payload = encode_payload::<C, _>(&ArithmeticError::Transport(format!("{err:?}")));
                        return Some(Box::pin(futures::stream::once(async move {
                            CallResponse::Error(error_payload)
                        })));
                    }
                };

                Some(Box::pin(self.inner.count(req).stream().map(|step| match step {
                    Step::Progress(progress) => CallResponse::Progress(encode_payload::<C, _>(&progress)),
                    Step::Success(result) => CallResponse::Success(encode_payload::<C, _>(&result)),
                    Step::Error(err) => CallResponse::Error(encode_payload::<C, _>(&err)),
                    Step::Transport(_) => {
                        CallResponse::Error(encode_payload::<C, _>(&ArithmeticError::Interrupted))
                    }
                })))
            }
            _ => None,
        }
    }
}

#[async_trait]
impl<I, C> MethodResponder for ArithmeticV1MethodResponder<I, C>
where
    I: ArithmeticV1 + Send + Sync,
    C: UnaryCodec + Send + Sync,
{
    fn get_name(&self) -> &str {
        "arithmetic_v1"
    }

    async fn execute(&self, method: &str, payload: Payload) -> Response {
        match method {
            "add" => {
                let req = match payload.decode::<C, AddRequest>() {
                    Ok(r) => r,
                    Err(e) => return Response::Error(e.surely_encode::<C>()),
                };
                match self.inner.add(req) {
                    Ok(result) => match result.encode::<C>() {
                        Ok(encoded) => Response::Success(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                    Err(err) => match err.encode::<C>() {
                        Ok(encoded) => Response::Error(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                }
            }
            "mul" => {
                let req = match payload.decode::<C, MulRequest>() {
                    Ok(r) => r,
                    Err(e) => return Response::Error(e.surely_encode::<C>()),
                };
                match self.inner.mul(req).await {
                    Ok(result) => match result.encode::<C>() {
                        Ok(encoded) => Response::Success(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                    Err(err) => match err.encode::<C>() {
                        Ok(encoded) => Response::Error(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                }
            }
            _ => Response::Error(TransportError::ModuleNotFound.surely_encode::<C>()),
        }
    }
}

impl<I, C> MethodResponderWithCodec for ArithmeticV1MethodResponder<I, C>
where
    I: ArithmeticV1 + Send + Sync,
    C: UnaryCodec + Send + Sync,
{
    type Codec = C;
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use transport::{CallResponse, CallResponseStream, ClientSideChunkedTransport, ClientSideOneShotTransport, Encodable, Error, JsonCodec, Payload, ProgressOutcome, TransportIssue};

    use crate::{AddRequest, ArithmeticV1Client, CountRequest, CountStep, MulRequest, MulResult, StepsMade};

    struct MockTransport {
        add_calls: AtomicUsize,
        mul_calls: AtomicUsize,
        count_calls: AtomicUsize,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                add_calls: AtomicUsize::new(0),
                mul_calls: AtomicUsize::new(0),
                count_calls: AtomicUsize::new(0),
            }
        }
    }

    impl ClientSideOneShotTransport<JsonCodec, AddRequest> for MockTransport {
        fn execute_method_call(&self, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call(&self, _module: &str, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            self.add_calls.fetch_add(1, Ordering::Relaxed);
            async move { MulResult { r: -1 }.encode::<JsonCodec>() }
        }

        fn execute_relayed_module_method_call(&self, _server: &str, _module: &str, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    impl ClientSideOneShotTransport<JsonCodec, MulRequest> for MockTransport {
        fn execute_method_call(&self, _method: &str, _payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call(&self, _module: &str, _method: &str, payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            self.mul_calls.fetch_add(1, Ordering::Relaxed);
            async move { MulResult { r: payload.a * payload.b }.encode::<JsonCodec>() }
        }

        fn execute_relayed_module_method_call(&self, _server: &str, _module: &str, _method: &str, _payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    impl ClientSideChunkedTransport<JsonCodec, CountRequest> for MockTransport {
        fn execute_method_call_stream(&self, _method: &str, _payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call_stream(&self, _module: &str, _method: &str, payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            self.count_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                let frames = vec![
                    Ok(CallResponse::Progress(
                        CountStep { currently_at: payload.from }.encode::<JsonCodec>()?,
                    )),
                    Ok(CallResponse::Success(
                        StepsMade { total: (payload.to - payload.from).abs() + 1 }.encode::<JsonCodec>()?,
                    )),
                ];
                Ok(Box::pin(futures::stream::iter(frames)) as CallResponseStream)
            }
        }

        fn execute_relayed_module_method_call_stream(&self, _server: &str, _module: &str, _method: &str, _payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    #[tokio::test]
    async fn mul_uses_mul_request_transport_impl() {
        let transport = MockTransport::new();

        let outcome = <MockTransport as ArithmeticV1Client<JsonCodec>>::mul(&transport, 3, 4)
            .await;

        let ProgressOutcome::Success(result) = outcome else {
            panic!("mul should succeed");
        };
        assert_eq!(result.r, 12);
        assert_eq!(transport.add_calls.load(Ordering::Relaxed), 0);
        assert_eq!(transport.mul_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn count_uses_count_request_stream_transport_impl() {
        let transport = MockTransport::new();

        let progressable_future = <MockTransport as ArithmeticV1Client<JsonCodec>>::count(&transport, 2, 4)
            .await
            .expect("count should start");

        let steps = progressable_future
            .await_result()
            .await;

        let ProgressOutcome::Success(steps) = steps else {
            panic!("count should finish successfully");
        };

        assert_eq!(steps.total, 3);
        assert_eq!(transport.add_calls.load(Ordering::Relaxed), 0);
        assert_eq!(transport.mul_calls.load(Ordering::Relaxed), 0);
        assert_eq!(transport.count_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn count_surfaces_transport_errors_separately() {
        struct FailingTransport;

        impl ClientSideOneShotTransport<JsonCodec, AddRequest> for FailingTransport {
            fn execute_method_call(&self, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }

            fn execute_module_method_call(&self, _module: &str, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }

            fn execute_relayed_module_method_call(&self, _server: &str, _module: &str, _method: &str, _payload: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }
        }

        impl ClientSideOneShotTransport<JsonCodec, MulRequest> for FailingTransport {
            fn execute_method_call(&self, _method: &str, _payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }

            fn execute_module_method_call(&self, _module: &str, _method: &str, _payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }

            fn execute_relayed_module_method_call(&self, _server: &str, _module: &str, _method: &str, _payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
                async { unreachable!("unused in test") }
            }
        }

        impl ClientSideChunkedTransport<JsonCodec, CountRequest> for FailingTransport {
            fn execute_method_call_stream(&self, _method: &str, _payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
                async { unreachable!("unused in test") }
            }

            fn execute_module_method_call_stream(&self, _module: &str, _method: &str, _payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
                async move {
                    Ok(Box::pin(futures::stream::iter(vec![
                        Err(Error::ReceiveResponse("stream failed".to_string())),
                    ])) as CallResponseStream)
                }
            }

            fn execute_relayed_module_method_call_stream(&self, _server: &str, _module: &str, _method: &str, _payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
                async { unreachable!("unused in test") }
            }
        }

        let outcome = <FailingTransport as ArithmeticV1Client<JsonCodec>>::count(&FailingTransport, 2, 4)
            .await
            .expect("count should start")
            .await_result()
            .await;

        match outcome {
            ProgressOutcome::Transport(TransportIssue::Error(Error::ReceiveResponse(message))) => {
                assert!(message.contains("stream failed"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    // A transport that returns:
    //   mul(a, b) = Ok(a * b)        for even `a`
    //   mul(a, b) = App error        for odd `a`
    //   mul(a, b) = transport error  when a == 99
    struct BatchTransport;

    impl ClientSideOneShotTransport<JsonCodec, AddRequest> for BatchTransport {
        fn execute_method_call(&self, _: &str, _: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send { async { unreachable!() } }
        fn execute_module_method_call(&self, _: &str, _: &str, _: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send { async { unreachable!() } }
        fn execute_relayed_module_method_call(&self, _: &str, _: &str, _: &str, _: AddRequest) -> impl Future<Output = Result<Payload, Error>> + Send { async { unreachable!() } }
    }

    impl ClientSideOneShotTransport<JsonCodec, MulRequest> for BatchTransport {
        fn execute_method_call(&self, _: &str, _: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send { async { unreachable!() } }

        fn execute_module_method_call(&self, _: &str, _: &str, payload: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send {
            async move {
                if payload.a == 99 {
                    return Err(Error::ReceiveResponse("network down".to_string()));
                }
                if payload.a % 2 != 0 {
                    use crate::ArithmeticError;
                    return Err(Error::App(ArithmeticError::Overflow.encode::<JsonCodec>()?));
                }
                MulResult { r: payload.a * payload.b }.encode::<JsonCodec>()
            }
        }

        fn execute_relayed_module_method_call(&self, _: &str, _: &str, _: &str, _: MulRequest) -> impl Future<Output = Result<Payload, Error>> + Send { async { unreachable!() } }
    }

    impl ClientSideChunkedTransport<JsonCodec, CountRequest> for BatchTransport {
        fn execute_method_call_stream(&self, _: &str, _: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send { async { unreachable!() } }
        fn execute_module_method_call_stream(&self, _: &str, _: &str, payload: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async move {
                let frames = (payload.from..=payload.to).map(|i| {
                    Ok(CallResponse::Progress(CountStep { currently_at: i }.encode::<JsonCodec>().unwrap()))
                }).chain(std::iter::once(
                    Ok(CallResponse::Success(StepsMade { total: payload.to - payload.from + 1 }.encode::<JsonCodec>().unwrap()))
                )).collect::<Vec<_>>();
                Ok(Box::pin(futures::stream::iter(frames)) as CallResponseStream)
            }
        }
        fn execute_relayed_module_method_call_stream(&self, _: &str, _: &str, _: &str, _: CountRequest) -> impl Future<Output = Result<CallResponseStream, Error>> + Send { async { unreachable!() } }
    }

    /// Batch loop: continue on app error, break on transport issue.
    /// Demonstrates that match arms control the embedding loop directly.
    #[tokio::test]
    async fn batch_loop_continues_on_app_error_breaks_on_transport() {
        use crate::ArithmeticError;

        let transport = BatchTransport;
        let inputs = [2_i32, 3, 4, 99, 6]; // 3 → app error, 99 → transport, 6 never reached
        let mut successes = vec![];
        let mut app_errors = 0_usize;
        let mut stopped_at: Option<i32> = None;

        'batch: for a in inputs {
            match <BatchTransport as ArithmeticV1Client<JsonCodec>>::mul(&transport, a, 10).await {
                ProgressOutcome::Success(r) => {
                    successes.push(r.r);
                }
                ProgressOutcome::Error(ArithmeticError::Overflow) => {
                    app_errors += 1;
                    continue 'batch; // skip this input, keep going
                }
                ProgressOutcome::Error(_) | ProgressOutcome::Transport(_) => {
                    stopped_at = Some(a);
                    break 'batch; // unrecoverable — abort the whole batch
                }
            }
        }

        assert_eq!(successes, vec![20, 40]);   // 2*10 and 4*10 succeeded
        assert_eq!(app_errors, 1);             // 3 caused Overflow
        assert_eq!(stopped_at, Some(99));      // transport error on 99 stopped the loop
    }

    /// Progressive stream: break early when a terminal step is received.
    /// The while-let loop ends naturally on Success/Error/Transport, and
    /// the caller can also break early from within Progress.
    #[tokio::test]
    async fn stream_loop_collects_progress_then_breaks_on_success() {
        use futures::{StreamExt, pin_mut};
        use progressable_future::Step;

        let transport = BatchTransport;
        let fut = <BatchTransport as ArithmeticV1Client<JsonCodec>>::count(&transport, 1, 5)
            .await
            .expect("count should start");

        let stream = fut.stream();
        pin_mut!(stream);

        let mut progress_values = vec![];
        let mut final_total: Option<i32> = None;

        while let Some(step) = stream.next().await {
            match step {
                Step::Progress(p) => {
                    progress_values.push(p.currently_at);
                    // could break here to stop early — the stream is just an iterator
                }
                Step::Success(r) => {
                    final_total = Some(r.total);
                    break; // explicit break, though stream ends naturally after Success
                }
                Step::Error(e) => {
                    panic!("unexpected error: {e:?}");
                }
                Step::Transport(t) => {
                    panic!("unexpected transport issue: {t:?}");
                }
            }
        }

        assert_eq!(progress_values, vec![1, 2, 3, 4, 5]);
        assert_eq!(final_total, Some(5));
    }

}

fn decode_one_shot_outcome<C, T, E>(result: Result<Payload, Error>) -> ProgressOutcome<T, E, Error>
where
    C: UnaryCodec,
    T: serde::de::DeserializeOwned,
    E: serde::de::DeserializeOwned,
{
    match result {
        Ok(payload) => match payload.decode::<C, T>() {
            Ok(value) => ProgressOutcome::Success(value),
            Err(error) => ProgressOutcome::Transport(TransportIssue::Error(error)),
        },
        Err(Error::App(payload)) => match payload.decode::<C, E>() {
            Ok(error) => ProgressOutcome::Error(error),
            Err(decode_error) => ProgressOutcome::Transport(TransportIssue::Error(decode_error)),
        },
        Err(error) => ProgressOutcome::Transport(TransportIssue::Error(error)),
    }
}

fn encode_payload<C, T>(value: &T) -> Payload
where
    C: UnaryCodec,
    T: serde::Serialize,
{
    match value.encode::<C>() {
        Ok(payload) => payload,
        Err(error) => ArithmeticError::Transport(format!("{error:?}"))
            .encode::<C>()
            .unwrap_or_else(|fallback_error| fallback_error.surely_encode::<C>()),
    }
}
