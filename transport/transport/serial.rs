use std::{future::Future, pin::Pin, sync::atomic::Ordering};

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{CallResponse, Error, Payload, Result, UnaryCodec};

use super::NonNegativeWrappingAtomicI64;
use super::{CallResponseStream, ChunkedTransport, OneShotTransport};

static FRAMECOUNTER: NonNegativeWrappingAtomicI64 = NonNegativeWrappingAtomicI64::new(1);

/// Request frame used by serial transports to correlate responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestFrame(i64, Payload);

impl RequestFrame {
    /// Creates a new request frame with a fresh positive request identifier.
    pub fn new(payload: Payload) -> Self {
        Self(FRAMECOUNTER.fetch_inc_wrap(Ordering::Relaxed), payload)
    }

    /// Returns the negative identifier used by response frames for this request.
    pub fn response_id(&self) -> i64 {
        -self.get_id()
    }

    /// Returns the request frame identifier.
    pub fn get_id(&self) -> i64 {
        self.0
    }

    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        &self.1
    }

    /// Returns whether this frame is a request frame.
    pub fn is_request(&self) -> bool {
        self.0 > 0
    }

    /// Returns whether this frame is a response frame.
    pub fn is_response(&self) -> bool {
        self.0 < 0
    }
}

/// Response variants emitted by serial transports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerialResponse {
    /// Non-terminal progress payload.
    Progress(Payload),
    /// Terminal success payload.
    Success(Payload),
    /// Terminal application-level error payload.
    AppError(Payload),
    /// Terminal transport-level error.
    Transport(Error),
}

impl SerialResponse {
    /// Returns whether this response ends the exchange.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Success(_) | Self::AppError(_) | Self::Transport(_)
        )
    }
}

impl From<CallResponse> for SerialResponse {
    fn from(value: CallResponse) -> Self {
        match value {
            CallResponse::Progress(payload) => Self::Progress(payload),
            CallResponse::Success(payload) => Self::Success(payload),
            CallResponse::Error(payload) => Self::AppError(payload),
        }
    }
}

/// Response frame used by serial transports to correlate results to requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame(i64, SerialResponse);

impl ResponseFrame {
    /// Creates a response frame addressed to the provided request frame.
    pub fn response_to(frame: &RequestFrame, response: SerialResponse) -> Self {
        Self(frame.response_id(), response)
    }

    /// Returns the raw response-frame identifier.
    pub fn get_id(&self) -> i64 {
        self.0
    }

    /// Returns the original request identifier this frame answers.
    pub fn get_request_id(&self) -> i64 {
        -self.0
    }

    /// Borrows the contained response value.
    pub fn get_response(&self) -> &SerialResponse {
        &self.1
    }

    /// Consumes the frame and returns the contained response value.
    pub fn into_response(self) -> SerialResponse {
        self.1
    }
}

/// Stream of serial responses for a single in-flight request.
pub type SerialResponseStream = Pin<Box<dyn Stream<Item = Result<SerialResponse>> + Send>>;

/// Bidirectional transport that exchanges framed serial requests and responses.
pub trait SerialTransport {
    /// Sends a request frame and returns the stream of responses associated with it.
    fn send_frame(
        &self,
        frame: RequestFrame,
    ) -> impl Future<Output = Result<SerialResponseStream>> + Send;
}

/// Server-side handle for a single inbound serial request frame.
pub trait ServerSideSerialTransportItem {
    /// Returns the received request frame.
    fn get_frame(&self) -> &RequestFrame;
    /// Completes the request with a terminal success payload.
    fn respond_with(self, payload: Payload) -> impl Future<Output = Result<()>> + Send;
    /// Completes the request with a terminal application-level error payload.
    fn respond_with_app_error(self, payload: Payload) -> impl Future<Output = Result<()>> + Send;
    /// Completes the request with a terminal transport-level error.
    fn respond_with_transport_error(self, error: Error) -> impl Future<Output = Result<()>> + Send;
    /// Streams zero or more progress frames followed by a terminal response.
    fn respond_with_stream<S>(self, stream: S) -> impl Future<Output = Result<()>> + Send
    where
        S: Stream<Item = CallResponse> + Send + 'static;
}

/// Server-side source of inbound serial transport items.
pub trait ServerSideSerialTransport {
    /// Item type yielded for each inbound request.
    type Item: ServerSideSerialTransportItem;
    /// Stream type used to yield inbound requests.
    type ItemStream: Stream<Item = Self::Item>;

    /// Returns the stream of inbound serial requests handled by this transport.
    fn get_incoming_stream(&self) -> impl Future<Output = Self::ItemStream> + Send;
}

impl<T: SerialTransport + Sync> OneShotTransport for T {
    async fn send_payload(&self, payload: Payload) -> Result<Payload> {
        let mut responses = self.send_frame(RequestFrame::new(payload)).await?;

        match responses.next().await {
            Some(Ok(SerialResponse::Success(payload))) => Ok(payload),
            Some(Ok(SerialResponse::AppError(payload))) => Err(Error::App(payload)),
            Some(Ok(SerialResponse::Transport(error))) => Err(error),
            Some(Ok(SerialResponse::Progress(_))) => Err(Error::ReceiveResponse(
                "Received streaming progress on one-shot serial request".to_string(),
            )),
            Some(Err(error)) => Err(error),
            None => Err(Error::ReceiveResponse(
                "Serial transport closed before sending a terminal response".to_string(),
            )),
        }
    }
}

impl<T: SerialTransport + Sync> ChunkedTransport for T {
    async fn send_payload_stream<C: UnaryCodec + Send + 'static>(
        &self,
        payload: Payload,
    ) -> Result<CallResponseStream> {
        let stream = self.send_frame(RequestFrame::new(payload)).await?;
        let stream = stream.map(|response| response.and_then(map_serial_response));
        Ok(Box::pin(stream) as CallResponseStream)
    }
}

fn map_serial_response(response: SerialResponse) -> Result<CallResponse> {
    match response {
        SerialResponse::Progress(payload) => Ok(CallResponse::Progress(payload)),
        SerialResponse::Success(payload) => Ok(CallResponse::Success(payload)),
        SerialResponse::AppError(payload) => Ok(CallResponse::Error(payload)),
        SerialResponse::Transport(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{StreamExt, stream};
    use tokio::sync::Mutex;

    use super::{RequestFrame, SerialResponse, SerialResponseStream, SerialTransport};
    use crate::{CallResponse, ChunkedTransport, Error, JsonCodec, OneShotTransport, Payload};

    struct MockSerialTransport {
        responses: Arc<Mutex<Option<Vec<crate::Result<SerialResponse>>>>>,
    }

    impl MockSerialTransport {
        fn new(responses: Vec<crate::Result<SerialResponse>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(Some(responses))),
            }
        }
    }

    impl SerialTransport for MockSerialTransport {
        async fn send_frame(&self, frame: RequestFrame) -> crate::Result<SerialResponseStream> {
            assert!(frame.is_request());
            let responses = self
                .responses
                .lock()
                .await
                .take()
                .expect("responses already taken");
            Ok(Box::pin(stream::iter(responses)) as SerialResponseStream)
        }
    }

    #[test]
    fn request_frames_use_positive_ids() {
        for _ in 0..8 {
            let frame = RequestFrame::new(Payload::new(b"request"));
            assert!(frame.is_request());
            assert!(!frame.is_response());
            assert!(frame.get_id() > 0);
        }
    }

    #[test]
    fn response_ids_negate_request_ids() {
        let request = RequestFrame::new(Payload::new(b"request"));
        assert_eq!(request.response_id(), -request.get_id());
    }

    #[tokio::test]
    async fn one_shot_maps_serial_success_and_app_errors() {
        let success_transport = MockSerialTransport::new(vec![Ok(SerialResponse::Success(
            Payload::new(br#"{"r":12}"#),
        ))]);
        let success = success_transport
            .send_payload(Payload::new(br#"{"a":3,"b":4}"#))
            .await
            .expect("success response");
        assert_eq!(success.get_bytes(), br#"{"r":12}"#);

        let app_error_transport = MockSerialTransport::new(vec![Ok(SerialResponse::AppError(
            Payload::new(br#""overflow""#),
        ))]);
        let error = app_error_transport
            .send_payload(Payload::new(br#"{"a":3,"b":4}"#))
            .await
            .expect_err("application error should bubble up");

        match error {
            Error::App(payload) => assert_eq!(payload.get_bytes(), br#""overflow""#),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn chunked_maps_serial_responses_into_call_responses() {
        let transport = MockSerialTransport::new(vec![
            Ok(SerialResponse::Progress(Payload::new(
                br#"{"currently_at":3}"#,
            ))),
            Ok(SerialResponse::Success(Payload::new(br#"{"total":4}"#))),
        ]);

        let mut stream =
            <MockSerialTransport as ChunkedTransport>::send_payload_stream::<JsonCodec>(
                &transport,
                Payload::new(b"count"),
            )
            .await
            .expect("stream should start");

        match stream.next().await.expect("progress").expect("valid") {
            CallResponse::Progress(p) => assert_eq!(p.get_bytes(), br#"{"currently_at":3}"#),
            other => panic!("unexpected: {other:?}"),
        }
        match stream.next().await.expect("success").expect("valid") {
            CallResponse::Success(p) => assert_eq!(p.get_bytes(), br#"{"total":4}"#),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn one_shot_rejects_progress_only_responses() {
        let transport = MockSerialTransport::new(vec![Ok(SerialResponse::Progress(Payload::new(
            br#"{"currently_at":1}"#,
        )))]);

        let error = transport
            .send_payload(Payload::new(b"count"))
            .await
            .expect_err("one-shot should reject progress frames");

        match error {
            Error::ReceiveResponse(message) => assert!(message.contains("one-shot serial request")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
