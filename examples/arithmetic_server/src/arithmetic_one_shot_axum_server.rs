use tokio::net::TcpListener;

use transport::{JsonCodec, ServerSideOneShotTransportItem, ServerSideOneShotTransport, AxumTransport};

pub async fn run_axum_one_shot () {

    let transport = AxumTransport::<JsonCodec>::new(128);

    let app = transport.router();

    let listener = TcpListener::bind("127.0.0.1:3300")
        .await
        .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .unwrap();
    });

    let mut stream = transport.get_incoming_stream().await;

    use futures::StreamExt;

    while let Some(payload) = stream.next().await {
        println!("Got payload: {:?}", payload.get_payload());
    }
}