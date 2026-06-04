use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::{StreamExt, pin_mut};
use progressable_future::Step;
use transport::{
    CallResponse, CallResponseStream, ClientSideChunkedTransport, ClientSideOneShotTransport,
    Encodable, Error, JsonCodec, Payload, ProgressOutcome, TransportIssue,
};

use crate::{
    AddRequest, ArithmeticError, ArithmeticV1Client, ArithmeticV1MethodPermissions, CountRequest,
    CountStep, MulRequest, MulResult, StepsMade,
};

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
    fn execute_method_call(
        &self,
        _method: &str,
        _payload: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!("unused in test") }
    }

    fn execute_module_method_call(
        &self,
        _module: &str,
        _method: &str,
        _payload: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        self.add_calls.fetch_add(1, Ordering::Relaxed);
        async move { MulResult { r: -1 }.encode::<JsonCodec>() }
    }

    fn execute_relayed_module_method_call(
        &self,
        _server: &str,
        _module: &str,
        _method: &str,
        _payload: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!("unused in test") }
    }
}

impl ClientSideOneShotTransport<JsonCodec, MulRequest> for MockTransport {
    fn execute_method_call(
        &self,
        _method: &str,
        _payload: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!("unused in test") }
    }

    fn execute_module_method_call(
        &self,
        _module: &str,
        _method: &str,
        payload: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        self.mul_calls.fetch_add(1, Ordering::Relaxed);
        async move {
            MulResult {
                r: payload.a * payload.b,
            }
            .encode::<JsonCodec>()
        }
    }

    fn execute_relayed_module_method_call(
        &self,
        _server: &str,
        _module: &str,
        _method: &str,
        _payload: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!("unused in test") }
    }
}

impl ClientSideChunkedTransport<JsonCodec, CountRequest> for MockTransport {
    fn execute_method_call_stream(
        &self,
        _method: &str,
        _payload: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        async { unreachable!("unused in test") }
    }

    fn execute_module_method_call_stream(
        &self,
        _module: &str,
        _method: &str,
        payload: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        self.count_calls.fetch_add(1, Ordering::Relaxed);
        async move {
            let frames = vec![
                Ok(CallResponse::Progress(
                    CountStep {
                        currently_at: payload.from,
                    }
                    .encode::<JsonCodec>()?,
                )),
                Ok(CallResponse::Success(
                    StepsMade {
                        total: (payload.to - payload.from).abs() + 1,
                    }
                    .encode::<JsonCodec>()?,
                )),
            ];
            Ok(Box::pin(futures::stream::iter(frames)) as CallResponseStream)
        }
    }

    fn execute_relayed_module_method_call_stream(
        &self,
        _server: &str,
        _module: &str,
        _method: &str,
        _payload: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        async { unreachable!("unused in test") }
    }
}

#[tokio::test]
async fn mul_uses_mul_request_transport_impl() {
    let transport = MockTransport::new();

    let outcome = <MockTransport as ArithmeticV1Client<JsonCodec>>::mul(
        &transport,
        MulRequest { a: 3, b: 4 },
    )
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

    let progressable_future = <MockTransport as ArithmeticV1Client<JsonCodec>>::count(
        &transport,
        CountRequest { from: 2, to: 4 },
    )
    .await
    .expect("count should start");

    let steps = progressable_future.await_result().await;

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
        fn execute_method_call(
            &self,
            _method: &str,
            _payload: AddRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call(
            &self,
            _module: &str,
            _method: &str,
            _payload: AddRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_relayed_module_method_call(
            &self,
            _server: &str,
            _module: &str,
            _method: &str,
            _payload: AddRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    impl ClientSideOneShotTransport<JsonCodec, MulRequest> for FailingTransport {
        fn execute_method_call(
            &self,
            _method: &str,
            _payload: MulRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call(
            &self,
            _module: &str,
            _method: &str,
            _payload: MulRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_relayed_module_method_call(
            &self,
            _server: &str,
            _module: &str,
            _method: &str,
            _payload: MulRequest,
        ) -> impl Future<Output = Result<Payload, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    impl ClientSideChunkedTransport<JsonCodec, CountRequest> for FailingTransport {
        fn execute_method_call_stream(
            &self,
            _method: &str,
            _payload: CountRequest,
        ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async { unreachable!("unused in test") }
        }

        fn execute_module_method_call_stream(
            &self,
            _module: &str,
            _method: &str,
            _payload: CountRequest,
        ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async move {
                Ok(
                    Box::pin(futures::stream::iter(vec![Err(Error::ReceiveResponse(
                        "stream failed".to_string(),
                    ))])) as CallResponseStream,
                )
            }
        }

        fn execute_relayed_module_method_call_stream(
            &self,
            _server: &str,
            _module: &str,
            _method: &str,
            _payload: CountRequest,
        ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
            async { unreachable!("unused in test") }
        }
    }

    let outcome = <FailingTransport as ArithmeticV1Client<JsonCodec>>::count(
        &FailingTransport,
        CountRequest { from: 2, to: 4 },
    )
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

struct BatchTransport;

impl ClientSideOneShotTransport<JsonCodec, AddRequest> for BatchTransport {
    fn execute_method_call(
        &self,
        _: &str,
        _: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!() }
    }

    fn execute_module_method_call(
        &self,
        _: &str,
        _: &str,
        _: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!() }
    }

    fn execute_relayed_module_method_call(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: AddRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!() }
    }
}

impl ClientSideOneShotTransport<JsonCodec, MulRequest> for BatchTransport {
    fn execute_method_call(
        &self,
        _: &str,
        _: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!() }
    }

    fn execute_module_method_call(
        &self,
        _: &str,
        _: &str,
        payload: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async move {
            if payload.a == 99 {
                return Err(Error::ReceiveResponse("network down".to_string()));
            }
            if payload.a % 2 != 0 {
                return Err(Error::App(ArithmeticError::Overflow.encode::<JsonCodec>()?));
            }
            MulResult {
                r: payload.a * payload.b,
            }
            .encode::<JsonCodec>()
        }
    }

    fn execute_relayed_module_method_call(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: MulRequest,
    ) -> impl Future<Output = Result<Payload, Error>> + Send {
        async { unreachable!() }
    }
}

impl ClientSideChunkedTransport<JsonCodec, CountRequest> for BatchTransport {
    fn execute_method_call_stream(
        &self,
        _: &str,
        _: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        async { unreachable!() }
    }

    fn execute_module_method_call_stream(
        &self,
        _: &str,
        _: &str,
        payload: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        async move {
            let frames = (payload.from..=payload.to)
                .map(|i| {
                    Ok(CallResponse::Progress(
                        CountStep { currently_at: i }.encode::<JsonCodec>().unwrap(),
                    ))
                })
                .chain(std::iter::once(Ok(CallResponse::Success(
                    StepsMade {
                        total: payload.to - payload.from + 1,
                    }
                    .encode::<JsonCodec>()
                    .unwrap(),
                ))))
                .collect::<Vec<_>>();
            Ok(Box::pin(futures::stream::iter(frames)) as CallResponseStream)
        }
    }

    fn execute_relayed_module_method_call_stream(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: CountRequest,
    ) -> impl Future<Output = Result<CallResponseStream, Error>> + Send {
        async { unreachable!() }
    }
}

#[tokio::test]
async fn batch_loop_continues_on_app_error_breaks_on_transport() {
    let transport = BatchTransport;
    let inputs = [2_i32, 3, 4, 99, 6];
    let mut successes = vec![];
    let mut app_errors = 0_usize;
    let mut stopped_at: Option<i32> = None;

    'batch: for a in inputs {
        match <BatchTransport as ArithmeticV1Client<JsonCodec>>::mul(
            &transport,
            MulRequest { a, b: 10 },
        )
        .await
        {
            ProgressOutcome::Success(r) => {
                successes.push(r.r);
            }
            ProgressOutcome::Error(ArithmeticError::Overflow) => {
                app_errors += 1;
                continue 'batch;
            }
            ProgressOutcome::Error(_) | ProgressOutcome::Transport(_) => {
                stopped_at = Some(a);
                break 'batch;
            }
        }
    }

    assert_eq!(successes, vec![20, 40]);
    assert_eq!(app_errors, 1);
    assert_eq!(stopped_at, Some(99));
}

#[tokio::test]
async fn stream_loop_collects_progress_then_breaks_on_success() {
    let transport = BatchTransport;
    let fut = <BatchTransport as ArithmeticV1Client<JsonCodec>>::count(
        &transport,
        CountRequest { from: 1, to: 5 },
    )
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
            }
            Step::Success(r) => {
                final_total = Some(r.total);
                break;
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

#[test]
fn exposes_allowed_roles_metadata() {
    // add is #[public] — no role restriction
    assert!(ArithmeticV1MethodPermissions::is_public("add"));
    assert_eq!(ArithmeticV1MethodPermissions::allowed_roles("add"), None);
    assert!(ArithmeticV1MethodPermissions::is_role_allowed(
        "add", "anyone"
    ));

    // mul requires "user"
    assert!(!ArithmeticV1MethodPermissions::is_public("mul"));
    assert_eq!(
        ArithmeticV1MethodPermissions::allowed_roles("mul"),
        Some(&["user"][..]),
    );
    assert!(ArithmeticV1MethodPermissions::is_role_allowed(
        "mul", "user"
    ));
    assert!(!ArithmeticV1MethodPermissions::is_role_allowed(
        "mul", "guest"
    ));

    // count requires "admin"
    assert!(!ArithmeticV1MethodPermissions::is_public("count"));
    assert_eq!(
        ArithmeticV1MethodPermissions::allowed_roles("count"),
        Some(&["admin"][..]),
    );
    assert!(ArithmeticV1MethodPermissions::is_role_allowed(
        "count", "admin"
    ));
    assert!(!ArithmeticV1MethodPermissions::is_role_allowed(
        "count", "user"
    ));

    // unknown method
    assert!(!ArithmeticV1MethodPermissions::is_public("missing"));
    assert_eq!(
        ArithmeticV1MethodPermissions::allowed_roles("missing"),
        None
    );
}

// ── access-control tests ─────────────────────────────────────────────────────

struct PermissiveImpl;

impl crate::ArithmeticV1 for PermissiveImpl {
    fn add(&self, req: AddRequest) -> Result<crate::AddResult, ArithmeticError> {
        Ok(crate::AddResult { r: req.a + req.b })
    }

    fn mul(
        &self,
        req: MulRequest,
    ) -> impl std::future::Future<Output = Result<MulResult, ArithmeticError>> + Send {
        async move { Ok(MulResult { r: req.a * req.b }) }
    }

    fn count(
        &self,
        req: CountRequest,
    ) -> progressable_future::ProgressableFuture<CountStep, StepsMade, ArithmeticError> {
        use progressable_future::ProgressableFuture;
        let from = req.from;
        let to = req.to;
        ProgressableFuture::new(move |sender| async move {
            for i in from..=to {
                sender.progress(CountStep { currently_at: i });
            }
            Ok(StepsMade {
                total: to - from + 1,
            })
        })
    }
}

fn make_responder() -> crate::ArithmeticV1MethodResponder<PermissiveImpl, transport::JsonCodec> {
    crate::ArithmeticV1MethodResponder::new(PermissiveImpl)
}

fn encode<T: serde::Serialize>(v: &T) -> transport::Payload {
    transport::Payload::new(&serde_json::to_vec(v).unwrap())
}

#[tokio::test]
async fn admin_cannot_call_mul() {
    let responder = make_responder();
    let response = execution::MethodResponder::execute(
        &responder,
        "mul",
        encode(&MulRequest { a: 2, b: 3 }),
        Some("admin"),
    )
    .await;

    // Agent Change 06/04/2026: Updated pattern from Response::Error(payload)+decode to Response::TransportError(err)
    // because AccessForbidden is now a TransportError variant, not an app-error payload.
    let execution::Response::TransportError(err) = response else {
        panic!("expected TransportError response");
    };
    assert!(
        matches!(err, transport::Error::AccessForbidden),
        "expected AccessForbidden, got {err:?}"
    );
}

#[tokio::test]
async fn user_cannot_call_count() {
    let responder = make_responder();
    // count is streaming — check via execute_streaming
    let stream = responder.execute_streaming(
        "count",
        encode(&CountRequest { from: 1, to: 3 }),
        Some("user"),
    );
    let stream = stream.expect("expected a stream back for a streaming method");
    pin_mut!(stream);
    let first_frame = stream
        .next()
        .await
        .expect("stream must yield at least one frame");
    let CallResponse::Error(p) = first_frame else {
        panic!("expected Error frame");
    };
    let err = p
        .decode::<transport::JsonCodec, transport::Error>()
        .unwrap();
    assert!(
        matches!(err, transport::Error::AccessForbidden),
        "expected AccessForbidden, got {err:?}"
    );
}

#[tokio::test]
async fn public_method_accessible_without_role() {
    let responder = make_responder();
    let response = execution::MethodResponder::execute(
        &responder,
        "add",
        encode(&AddRequest { a: 1, b: 2 }),
        None,
    )
    .await;
    assert!(
        matches!(response, execution::Response::Success(_)),
        "public method should be callable without a role"
    );
}

#[tokio::test]
async fn anonymous_cannot_call_mul() {
    let responder = make_responder();
    let response = execution::MethodResponder::execute(
        &responder,
        "mul",
        encode(&MulRequest { a: 3, b: 4 }),
        None,
    )
    .await;
    let execution::Response::TransportError(err) = response else {
        panic!("expected TransportError response for anonymous caller on role-restricted method");
    };
    assert!(
        matches!(err, transport::Error::AccessForbidden),
        "expected AccessForbidden, got {err:?}"
    );
}

#[tokio::test]
async fn anonymous_cannot_call_count() {
    let responder = make_responder();
    let stream =
        responder.execute_streaming("count", encode(&CountRequest { from: 1, to: 3 }), None);
    let stream = stream.expect("expected a stream back for streaming method");
    pin_mut!(stream);
    let first_frame = stream
        .next()
        .await
        .expect("stream must yield at least one frame");
    let CallResponse::Error(p) = first_frame else {
        panic!("expected Error frame for anonymous caller on role-restricted streaming method");
    };
    let err = p
        .decode::<transport::JsonCodec, transport::Error>()
        .unwrap();
    assert!(
        matches!(err, transport::Error::AccessForbidden),
        "expected AccessForbidden, got {err:?}"
    );
}
