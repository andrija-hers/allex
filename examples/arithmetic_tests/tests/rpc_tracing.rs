//! The spans and events the generated client and responder emit, over HTTP: names, targets,
//! levels, messages and the `outcome` field, as a subscriber sees them.

mod common;
use common::start_http_server;

use std::sync::{Arc, Mutex, OnceLock};

use arithmetic_base::{AddRequest, ArithmeticV1Client, CountRequest};
use futures::{StreamExt, pin_mut};
use progressable_future::Step;
use transport::rpc_trace::{CLIENT, SERVER};
use transport::tracing::field::{Field, Visit};
use transport::tracing::span::{Attributes, Id, Record};
use transport::tracing::{Event, Level, Metadata, Subscriber};
use transport::{HttpTransport, JsonCodec, ProgressOutcome};

#[derive(Clone, Debug)]
struct SpanSeen {
    name: String,
    target: String,
    method: String,
    kind: String,
}

#[derive(Clone, Debug)]
struct EventSeen {
    target: String,
    level: Level,
    message: String,
    outcome: String,
    /// The `rpc_method` of the explicit parent span, if the event names one.
    parent_method: Option<String>,
}

#[derive(Default)]
struct Seen {
    spans: Vec<SpanSeen>,
    events: Vec<EventSeen>,
}

#[derive(Default)]
struct Fields {
    method: String,
    kind: String,
    message: String,
    outcome: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "rpc_method" => self.method = value.to_string(),
            "rpc_kind" => self.kind = value.to_string(),
            "outcome" => self.outcome = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

struct Recorder(Arc<Mutex<Seen>>);

impl Subscriber for Recorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let mut seen = self.0.lock().unwrap();
        seen.spans.push(SpanSeen {
            name: attrs.metadata().name().to_string(),
            target: attrs.metadata().target().to_string(),
            method: fields.method,
            kind: fields.kind,
        });
        Id::from_u64(seen.spans.len() as u64)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut seen = self.0.lock().unwrap();
        let parent_method = event
            .parent()
            .map(|id| seen.spans[id.into_u64() as usize - 1].method.clone());
        seen.events.push(EventSeen {
            target: event.metadata().target().to_string(),
            level: *event.metadata().level(),
            message: fields.message,
            outcome: fields.outcome,
            parent_method,
        });
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

fn recorder() -> Arc<Mutex<Seen>> {
    static SEEN: OnceLock<Arc<Mutex<Seen>>> = OnceLock::new();
    SEEN.get_or_init(|| {
        let seen = Arc::new(Mutex::new(Seen::default()));
        transport::tracing::subscriber::set_global_default(Recorder(seen.clone()))
            .expect("the only subscriber of this test binary");
        seen
    })
    .clone()
}

fn count_events(seen: &Seen, target: &str, level: Level, message: &str, outcome: &str) -> usize {
    seen.events
        .iter()
        .filter(|e| {
            e.target == target && e.level == level && e.message == message && e.outcome == outcome
        })
        .count()
}

#[tokio::test]
async fn generated_code_traces_unary_and_streaming_calls() {
    let seen = recorder();
    let base_url = start_http_server().await;
    let transport = HttpTransport::new(&base_url);

    match <HttpTransport as ArithmeticV1Client<JsonCodec>>::add(
        &transport,
        AddRequest { a: 3, b: 4 },
    )
    .await
    {
        ProgressOutcome::Success(r) => assert_eq!(r.r, 7),
        other => panic!("unexpected: {other:?}"),
    }

    let fut = <HttpTransport as ArithmeticV1Client<JsonCodec>>::count(
        &transport,
        CountRequest { from: 1, to: 4 },
    )
    .await
    .expect("count should start");
    let stream = fut.stream();
    pin_mut!(stream);
    while let Some(step) = stream.next().await {
        match step {
            Step::Progress(_) => {}
            Step::Success(r) => {
                assert_eq!(r.total, 4);
                break;
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    let seen = seen.lock().unwrap();
    for (name, method, kind) in [
        (CLIENT, "add", "unary"),
        (SERVER, "add", "unary"),
        (CLIENT, "count", "streaming"),
        (SERVER, "count", "streaming"),
    ] {
        assert!(
            seen.spans.iter().any(|s| s.name == name
                && s.target == name
                && s.method == method
                && s.kind == kind),
            "no {name} span for {method}: {:?}",
            seen.spans
        );
    }

    let finished = "rpc unary client call finished";
    assert_eq!(
        count_events(&seen, CLIENT, Level::DEBUG, finished, "success"),
        1
    );
    assert!(
        seen.events
            .iter()
            .any(|e| e.message == finished && e.parent_method.as_deref() == Some("add")),
        "the client event's parent is the call's span"
    );
    let started = "rpc unary server call started";
    assert_eq!(count_events(&seen, SERVER, Level::DEBUG, started, ""), 1);
    let finished = "rpc unary server call finished";
    assert_eq!(
        count_events(&seen, SERVER, Level::DEBUG, finished, "success"),
        1
    );

    let started = "rpc streaming client call started";
    assert_eq!(count_events(&seen, CLIENT, Level::DEBUG, started, ""), 1);
    let progress = "rpc streaming client progress";
    assert_eq!(
        count_events(&seen, CLIENT, Level::TRACE, progress, "progress"),
        4
    );
    let finished = "rpc streaming client call finished";
    assert_eq!(
        count_events(&seen, CLIENT, Level::DEBUG, finished, "success"),
        1
    );
    let started = "rpc streaming server call started";
    assert_eq!(count_events(&seen, SERVER, Level::DEBUG, started, ""), 1);
    let progress = "rpc streaming server progress";
    assert_eq!(
        count_events(&seen, SERVER, Level::TRACE, progress, "progress"),
        4
    );
    let finished = "rpc streaming server call finished";
    assert_eq!(
        count_events(&seen, SERVER, Level::DEBUG, finished, "success"),
        1
    );
    assert!(
        seen.events
            .iter()
            .filter(|e| e.target == CLIENT || e.target == SERVER)
            .filter(|e| e.message.starts_with("rpc streaming"))
            .all(|e| e.parent_method.as_deref() == Some("count")),
        "every streaming event names the call's span as its parent"
    );
}
