use std::{collections::HashMap, marker::PhantomData, net::SocketAddr, sync::{Arc, Mutex}};

use futures::{Sink, SinkExt, Stream, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{self, Message},
};

use crate::{CallResponse, Error, Result, UnaryCodec};

use super::{RequestFrame, SerialResponse, ResponseFrame, SerialResponseStream, SerialTransport,
    ServerSideSerialTransport, ServerSideSerialTransportItem};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WebSocketSerialMessage {
    Request(RequestFrame),
    Response(ResponseFrame),
}

pub struct WebSocketSerialClient<C: UnaryCodec> {
    pending: Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Result<SerialResponse>>>>>,
    outgoing: mpsc::UnboundedSender<WebSocketSerialMessage>,
    _codec: PhantomData<C>,
}

impl<C> WebSocketSerialClient<C>
where
    C: UnaryCodec + Send + Sync + 'static,
{
    pub async fn connect(url: &str) -> Result<Self> {
        let (stream, _) = connect_async(url)
            .await
            .map_err(|e| Error::SendRequest(format!("Could not connect to {url}: {e}")))?;

        let (write, read) = stream.split();
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();

        tokio::spawn(client_writer_loop::<C>(write, outgoing_rx, pending.clone()));
        tokio::spawn(client_reader_loop::<C>(read, pending.clone()));

        Ok(Self { pending, outgoing, _codec: PhantomData })
    }
}

impl<C> SerialTransport for WebSocketSerialClient<C>
where
    C: UnaryCodec + Send + Sync + 'static,
{
    async fn send_frame(&self, frame: RequestFrame) -> Result<SerialResponseStream> {
        let request_id = frame.get_id();
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending.lock().unwrap().insert(request_id, tx);

        if self.outgoing.send(WebSocketSerialMessage::Request(frame)).is_err() {
            self.pending.lock().unwrap().remove(&request_id);
            return Err(Error::SendRequest("WebSocket writer is not available".to_string()));
        }

        Ok(Box::pin(UnboundedReceiverStream::new(rx)) as SerialResponseStream)
    }
}

pub struct WebSocketSerialServer<C: UnaryCodec> {
    incoming: Mutex<Option<mpsc::Receiver<WebSocketSerialItem>>>,
    local_addr: SocketAddr,
    _codec: PhantomData<C>,
}

impl<C> WebSocketSerialServer<C>
where
    C: UnaryCodec + Send + Sync + 'static,
{
    pub async fn bind(addr: &str, buffer: usize) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::SendRequest(format!("Could not bind on {addr}: {e}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| Error::SendRequest(format!("Could not get local addr for {addr}: {e}")))?;
        let (incoming_tx, incoming_rx) = mpsc::channel(buffer);

        tokio::spawn(server_accept_loop::<C>(listener, incoming_tx));

        Ok(Self {
            incoming: Mutex::new(Some(incoming_rx)),
            local_addr,
            _codec: PhantomData,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<C> ServerSideSerialTransport for WebSocketSerialServer<C>
where
    C: UnaryCodec + Send + Sync + 'static,
{
    type Item = WebSocketSerialItem;
    type ItemStream = ReceiverStream<Self::Item>;

    async fn get_incoming_stream(&self) -> Self::ItemStream {
        let mut guard = self.incoming.lock().unwrap();
        let receiver = guard.take().expect("incoming stream already taken");
        ReceiverStream::new(receiver)
    }
}

pub struct WebSocketSerialItem {
    frame: RequestFrame,
    outgoing: mpsc::UnboundedSender<WebSocketSerialMessage>,
}

impl ServerSideSerialTransportItem for WebSocketSerialItem {
    fn get_frame(&self) -> &RequestFrame {
        &self.frame
    }

    async fn respond_with(self, payload: crate::Payload) -> Result<()> {
        self.send_response(SerialResponse::Success(payload))
    }

    async fn respond_with_app_error(self, payload: crate::Payload) -> Result<()> {
        self.send_response(SerialResponse::AppError(payload))
    }

    async fn respond_with_transport_error(self, error: Error) -> Result<()> {
        self.send_response(SerialResponse::Transport(error))
    }

    async fn respond_with_stream<S>(self, stream: S) -> Result<()>
    where
        S: Stream<Item = CallResponse> + Send + 'static,
    {
        let mut sent_terminal = false;
        pin_mut!(stream);

        while let Some(response) = stream.next().await {
            let serial_response = SerialResponse::from(response);
            sent_terminal = serial_response.is_terminal();
            self.send_response_ref(serial_response)?;
            if sent_terminal {
                break;
            }
        }

        if sent_terminal {
            Ok(())
        } else {
            self.send_response_ref(SerialResponse::Transport(Error::ReceiveResponse(
                "Streaming handler ended without a terminal frame".to_string(),
            )))
        }
    }
}

impl WebSocketSerialItem {
    fn send_response(self, response: SerialResponse) -> Result<()> {
        self.send_response_ref(response)
    }

    fn send_response_ref(&self, response: SerialResponse) -> Result<()> {
        self.outgoing
            .send(WebSocketSerialMessage::Response(ResponseFrame::response_to(
                &self.frame,
                response,
            )))
            .map_err(|_| Error::SendRequest("WebSocket peer dropped the response channel".to_string()))
    }
}

async fn server_accept_loop<C>(listener: TcpListener, incoming_tx: mpsc::Sender<WebSocketSerialItem>)
where
    C: UnaryCodec + Send + Sync + 'static,
{
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(server_connection_loop::<C>(stream, incoming_tx.clone()));
    }
}

async fn server_connection_loop<C>(stream: TcpStream, incoming_tx: mpsc::Sender<WebSocketSerialItem>)
where
    C: UnaryCodec + Send + Sync + 'static,
{
    let websocket = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(_) => return,
    };

    let (write, mut read) = websocket.split();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
    tokio::spawn(server_writer_loop::<C>(write, outgoing_rx));

    while let Some(message) = read.next().await {
        let message = match message {
            Ok(m) => m,
            Err(_) => break,
        };
        let message = match decode_websocket_message::<C>(message) {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(_) => break,
        };
        if let WebSocketSerialMessage::Request(frame) = message {
            let item = WebSocketSerialItem { frame, outgoing: outgoing_tx.clone() };
            if incoming_tx.send(item).await.is_err() {
                break;
            }
        }
    }
}

async fn client_reader_loop<C>(
    mut read: impl Stream<Item = core::result::Result<Message, tungstenite::Error>> + Unpin,
    pending: Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Result<SerialResponse>>>>>,
) where
    C: UnaryCodec + Send + Sync + 'static,
{
    while let Some(message) = read.next().await {
        let message = match message {
            Ok(m) => m,
            Err(e) => {
                fail_pending(&pending, Error::ReceiveResponse(format!("WebSocket receive error: {e}")));
                return;
            }
        };
        let message = match decode_websocket_message::<C>(message) {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => {
                fail_pending(&pending, e);
                return;
            }
        };
        if let WebSocketSerialMessage::Response(response_frame) = message {
            let request_id = response_frame.get_request_id();
            let response = response_frame.into_response();
            let is_terminal = response.is_terminal();

            let sender = {
                let mut pending = pending.lock().unwrap();
                if is_terminal {
                    pending.remove(&request_id)
                } else {
                    pending.get(&request_id).cloned()
                }
            };

            if let Some(sender) = sender {
                let _ = sender.send(Ok(response));
            }
        }
    }

    fail_pending(&pending, Error::ReceiveResponse("WebSocket connection closed".to_string()));
}

async fn client_writer_loop<C>(
    write: impl Sink<Message, Error = tungstenite::Error> + Unpin,
    mut outgoing_rx: mpsc::UnboundedReceiver<WebSocketSerialMessage>,
    pending: Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Result<SerialResponse>>>>>,
) where
    C: UnaryCodec + Send + Sync + 'static,
{
    if writer_loop::<C>(write, &mut outgoing_rx).await.is_err() {
        fail_pending(
            &pending,
            Error::SendRequest("WebSocket connection is no longer writable".to_string()),
        );
    }
}

async fn server_writer_loop<C>(
    write: impl Sink<Message, Error = tungstenite::Error> + Unpin,
    mut outgoing_rx: mpsc::UnboundedReceiver<WebSocketSerialMessage>,
) where
    C: UnaryCodec + Send + Sync + 'static,
{
    let _ = writer_loop::<C>(write, &mut outgoing_rx).await;
}

async fn writer_loop<C>(
    mut write: impl Sink<Message, Error = tungstenite::Error> + Unpin,
    outgoing_rx: &mut mpsc::UnboundedReceiver<WebSocketSerialMessage>,
) -> Result<()>
where
    C: UnaryCodec + Send + Sync + 'static,
{
    while let Some(message) = outgoing_rx.recv().await {
        let encoded = encode_websocket_message::<C>(&message)?;
        write
            .send(encoded)
            .await
            .map_err(|e| Error::SendRequest(format!("Could not write WebSocket frame: {e}")))?;
    }
    Ok(())
}

fn encode_websocket_message<C: UnaryCodec>(message: &WebSocketSerialMessage) -> Result<Message> {
    let payload = C::encode(message)?;
    Ok(Message::Binary(payload.get_bytes_forever().into()))
}

fn decode_websocket_message<C: UnaryCodec>(message: Message) -> Result<Option<WebSocketSerialMessage>> {
    match message {
        Message::Binary(bytes) => C::decode(bytes.as_ref()).map(Some),
        Message::Text(text) => C::decode(text.as_bytes()).map(Some),
        Message::Close(_) | Message::Ping(_) | Message::Pong(_) => Ok(None),
        _ => Ok(None),
    }
}

fn fail_pending(
    pending: &Arc<Mutex<HashMap<i64, mpsc::UnboundedSender<Result<SerialResponse>>>>>,
    error: Error,
) {
    let senders = {
        let mut pending = pending.lock().unwrap();
        pending.drain().map(|(_, sender)| sender).collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err(error.clone()));
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::{WebSocketSerialClient, WebSocketSerialServer};
    use crate::{
        CallResponse, ChunkedTransport, Error, JsonCodec, OneShotTransport, Payload,
        ServerSideSerialTransport, ServerSideSerialTransportItem,
    };

    #[tokio::test]
    async fn websocket_serial_round_trips_unary_and_streaming_responses() {
        let server = WebSocketSerialServer::<JsonCodec>::bind("127.0.0.1:0", 8)
            .await
            .expect("server should bind");
        let address = server.local_addr();

        let server_task = tokio::spawn(async move {
            let mut incoming = server.get_incoming_stream().await;
            while let Some(item) = incoming.next().await {
                match item.get_frame().get_payload().get_bytes() {
                    b"sum" => {
                        item.respond_with(Payload::new(br#"{"r":12}"#)).await.expect("unary response");
                    }
                    b"count" => {
                        let stream = futures::stream::iter(vec![
                            CallResponse::Progress(Payload::new(br#"{"currently_at":3}"#)),
                            CallResponse::Success(Payload::new(br#"{"total":4}"#)),
                        ]);
                        item.respond_with_stream(stream).await.expect("streaming response");
                    }
                    _ => {
                        item.respond_with_transport_error(Error::ModuleNotFound)
                            .await
                            .expect("transport error response");
                    }
                }
            }
        });

        let client = WebSocketSerialClient::<JsonCodec>::connect(&format!("ws://{address}"))
            .await
            .expect("client should connect");

        let unary = client.send_payload(Payload::new(b"sum")).await.expect("unary call");
        assert_eq!(unary.get_bytes(), br#"{"r":12}"#);

        let mut stream = <WebSocketSerialClient<JsonCodec> as ChunkedTransport>::send_payload_stream::<JsonCodec>(
            &client,
            Payload::new(b"count"),
        )
        .await
        .expect("streaming call should start");

        match stream.next().await.expect("progress").expect("valid progress") {
            CallResponse::Progress(p) => assert_eq!(p.get_bytes(), br#"{"currently_at":3}"#),
            other => panic!("unexpected: {other:?}"),
        }
        match stream.next().await.expect("success").expect("valid success") {
            CallResponse::Success(p) => assert_eq!(p.get_bytes(), br#"{"total":4}"#),
            other => panic!("unexpected: {other:?}"),
        }

        server_task.abort();
    }
}
