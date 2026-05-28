mod common;
use common::start_websocket_server;

use futures::{StreamExt, pin_mut};
use progressable_future::Step;
use transport::{JsonCodec, ProgressOutcome, WebSocketSerialClient};
use arithmetic_base::ArithmeticV1Client;

#[tokio::test]
async fn add_over_websocket() {
    let address = start_websocket_server().await;
    let transport = WebSocketSerialClient::<JsonCodec>::connect(&format!("ws://{address}"))
        .await
        .expect("client should connect");

    match <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::add(&transport, 3, 4).await {
        ProgressOutcome::Success(r) => assert_eq!(r.r, 7),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn mul_over_websocket() {
    let address = start_websocket_server().await;
    let transport = WebSocketSerialClient::<JsonCodec>::connect(&format!("ws://{address}"))
        .await
        .expect("client should connect");

    match <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::mul(&transport, 3, 4).await {
        ProgressOutcome::Success(r) => assert_eq!(r.r, 12),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn count_over_websocket() {
    let address = start_websocket_server().await;
    let transport = WebSocketSerialClient::<JsonCodec>::connect(&format!("ws://{address}"))
        .await
        .expect("client should connect");

    let fut = <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::count(&transport, 1, 4)
        .await
        .expect("count should start");

    let stream = fut.stream();
    pin_mut!(stream);

    let mut progress_values = vec![];
    let mut total: Option<i32> = None;

    while let Some(step) = stream.next().await {
        match step {
            Step::Progress(p) => progress_values.push(p.currently_at),
            Step::Success(r)  => { total = Some(r.total); break; }
            other             => panic!("unexpected: {other:?}"),
        }
    }

    assert_eq!(progress_values, vec![1, 2, 3, 4]);
    assert_eq!(total, Some(4));
}
