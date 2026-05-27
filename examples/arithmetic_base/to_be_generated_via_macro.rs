use std::{future::Future, pin::Pin};

use futures::{Stream, StreamExt};
use progressable_future::{ProgressableFuture, Step};
use transport::{CallResponse, ClientSideChunkedTransport, Encodable, Error, Payload, UnaryCodec};
use crate::{AddRequest, AddResult, ArithmeticError, CountRequest, CountStep, MulRequest, MulResult, StepsMade};

pub trait ArithmeticV1Client<C: UnaryCodec> {
    fn add(&self, first: i32, second: i32) -> impl Future<Output = Result<AddResult, Error>> + Send;
    fn mul(&self, first: i32, second: i32) -> impl Future<Output = Result<MulResult, Error>> + Send;
    fn count(&self, from: i32, to: i32) -> impl Future<Output = Result<ProgressableFuture<CountStep, StepsMade, ArithmeticError>, Error>> + Send;
}


use transport::{ClientSideOneShotTransport};

impl<T, C> ArithmeticV1Client<C> for T
where
    T: ClientSideOneShotTransport<C, AddRequest> + Sync
        + ClientSideOneShotTransport<C, MulRequest>
        + ClientSideChunkedTransport<C, CountRequest>,
    C: UnaryCodec + Send + Sync + 'static, {
    async fn add(&self, first: i32, second: i32) -> Result<AddResult, Error> {
        let result = 
        <T as ClientSideOneShotTransport<C, AddRequest>>::execute_module_method_call(
            self, 
            "arithmetic_v1", 
            "add", 
            AddRequest{a: first, b: second})
            .await?.
            decode::<C, AddResult>()?;
            Ok(result)
    } 
    async fn mul(&self, first: i32, second: i32) -> Result<MulResult, Error> {
        let result = 
        <T as ClientSideOneShotTransport<C, MulRequest>>::execute_module_method_call(
            self, 
            "arithmetic_v1", 
            "mul", 
            MulRequest{a: first, b: second})
            .await?.
            decode::<C, MulResult>()?;
            Ok(result)
    }

    async fn count(&self, from: i32, to: i32) -> Result<ProgressableFuture<CountStep, StepsMade, ArithmeticError>, Error> {
        let response_stream =
            <T as ClientSideChunkedTransport<C, CountRequest>>::execute_module_method_call_stream(
                self,
                "arithmetic_v1",
                "count",
                CountRequest { from, to },
            )
            .await?;

        Ok(ProgressableFuture::new(move |sender| async move {
            let mut response_stream = response_stream;

            while let Some(frame) = response_stream.next().await {
                match frame.map_err(transport_error_to_arithmetic)? {
                    CallResponse::Progress(payload) => {
                        let progress = payload
                            .decode::<C, CountStep>()
                            .map_err(transport_error_to_arithmetic)?;
                        sender.progress(progress);
                    }
                    CallResponse::Success(payload) => {
                        return payload
                            .decode::<C, StepsMade>()
                            .map_err(transport_error_to_arithmetic);
                    }
                    CallResponse::Error(payload) => {
                        return Err(payload
                            .decode::<C, ArithmeticError>()
                            .map_err(transport_error_to_arithmetic)?);
                    }
                }
            }

            Err(ArithmeticError::Interrupted)
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
                    Step::MissedSome | Step::Canceled | Step::Dropped => {
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

    use transport::{CallResponse, CallResponseStream, ClientSideOneShotTransport, ClientSideChunkedTransport, Encodable, Error, JsonCodec, Payload};

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

        let result = <MockTransport as ArithmeticV1Client<JsonCodec>>::mul(&transport, 3, 4)
            .await
            .expect("mul should succeed");

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
            .await
            .expect("count should finish");

        assert_eq!(steps.total, 3);
        assert_eq!(transport.add_calls.load(Ordering::Relaxed), 0);
        assert_eq!(transport.mul_calls.load(Ordering::Relaxed), 0);
        assert_eq!(transport.count_calls.load(Ordering::Relaxed), 1);
    }
}

fn transport_error_to_arithmetic(error: Error) -> ArithmeticError {
    ArithmeticError::Transport(format!("{error:?}"))
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
