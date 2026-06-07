use crate::transport::OneShotTransport;
use crate::transport_types::*;
use futures::{Stream, StreamExt};
use reqwest::StatusCode;
use serde::Serialize;
use std::{future::Future, pin::Pin};

/// HTTP transport that sends unary and chunked RPC requests over `POST /rpc`.
pub struct HttpTransport {
    base_url: String,
    bearer: Option<String>,
}

impl HttpTransport {
    /// Creates a new transport rooted at the supplied server base URL.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: format!("{}/rpc", base_url),
            bearer: None,
        }
    }

    /// Creates a transport that attaches `Authorization: Bearer <token>` to every request.
    pub fn new_with_bearer(base_url: &str, token: impl Into<String>) -> Self {
        Self {
            base_url: format!("{}/rpc", base_url),
            bearer: Some(token.into()),
        }
    }
}

/// Stream of chunked call responses produced by HTTP transports.
pub type CallResponseStream = Pin<Box<dyn Stream<Item = Result<CallResponse>> + Send>>;

/// Minimal transport for streaming RPC responses.
pub trait ChunkedTransport {
    /// Sends a serialized request payload and resolves with a stream of response frames.
    fn send_payload_stream<C: UnaryCodec + Send + 'static>(
        &self,
        payload: Payload,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send;
}

trait ClientSideChunkedTransportInternal<C: UnaryCodec, S: Serialize> {
    fn send_encoded_stream(
        &self,
        thingy: Result<S>,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send;
}

impl<T: ChunkedTransport + Sync, C: UnaryCodec + Send + 'static, S: Serialize + Send>
    ClientSideChunkedTransportInternal<C, S> for T
{
    fn send_encoded_stream(
        &self,
        thingy: Result<S>,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send {
        async move {
            match thingy {
                Ok(encoded_thingy) => match encoded_thingy.encode::<C>() {
                    Ok(encoded) => self.send_payload_stream::<C>(encoded).await,
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        }
    }
}

/// Convenience client helpers built on top of [`ChunkedTransport`].
pub trait ClientSideChunkedTransport<C: UnaryCodec, P: Serialize> {
    /// Executes a bare method call and returns the streamed response frames.
    fn execute_method_call_stream(
        &self,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send;
    /// Executes a module-scoped method call and returns the streamed response frames.
    fn execute_module_method_call_stream(
        &self,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send;
    /// Executes a relayed module call on another named server and returns the streamed response frames.
    fn execute_relayed_module_method_call_stream(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send;
}

impl<T: ChunkedTransport + Sync, C: UnaryCodec + Send + 'static, P: Serialize + Send>
    ClientSideChunkedTransport<C, P> for T
{
    fn execute_method_call_stream(
        &self,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send {
        <T as ClientSideChunkedTransportInternal<C, MethodCall>>::send_encoded_stream(
            self,
            MethodCall::new::<C, P>(method, payload),
        )
    }

    fn execute_module_method_call_stream(
        &self,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send {
        <T as ClientSideChunkedTransportInternal<C, ModuleMethodCall>>::send_encoded_stream(
            self,
            ModuleMethodCall::new::<C, P>(module, method, payload),
        )
    }

    fn execute_relayed_module_method_call_stream(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send {
        <T as ClientSideChunkedTransportInternal<C, RelayedModuleMethodCall>>::send_encoded_stream(
            self,
            RelayedModuleMethodCall::new::<C, P>(server, module, method, payload),
        )
    }
}

impl OneShotTransport for HttpTransport {
    fn send_payload(&self, payload: Payload) -> impl Future<Output = Result<Payload>> + Send {
        let url = self.base_url.clone();
        let bearer = self.bearer.clone();

        async move {
            let client = reqwest::Client::new();
            let mut req = client.post(&url).json(&payload);
            if let Some(token) = &bearer {
                req = req.bearer_auth(token);
            }

            let response = req.send().await.map_err(|e| {
                Error::SendRequest(format!(
                    "Could not send {} bytes to {}: {}",
                    payload.len(),
                    url,
                    e.to_string()
                ))
            })?;

            let status = response.status();
            let body = response.bytes().await.map_err(|e| {
                Error::ReceiveResponse(format!("Could not receive from {}: {}", url, e))
            })?;

            // HTTP 422 = app-level error. New servers return the raw encoded app error bytes
            // directly for readability; older servers wrap them in the transport Payload JSON shape.
            if status == StatusCode::UNPROCESSABLE_ENTITY {
                return Err(Error::App(decode_app_error_body(&body)));
            }

            // Agent Change 06/04/2026: HTTP 403 = AccessForbidden transport error. Return Err(Error::AccessForbidden) directly.

            if status == StatusCode::FORBIDDEN {
                return Err(Error::AccessForbidden);
            }

            if !status.is_success() {
                return Err(http_status_error(&url, status, &body));
            }

            let payload = serde_json::from_slice::<Payload>(&body).map_err(|e| {
                Error::ReceiveResponse(format!("Could not decode payload from {}: {}", url, e))
            })?;

            Ok(payload)
        }
    }
}

impl ChunkedTransport for HttpTransport {
    fn send_payload_stream<C: UnaryCodec + Send + 'static>(
        &self,
        payload: Payload,
    ) -> impl Future<Output = Result<CallResponseStream>> + Send {
        let url = self.base_url.clone();
        let bearer = self.bearer.clone();

        async move {
            let client = reqwest::Client::new();
            let mut req = client.post(&url).json(&payload);
            if let Some(token) = &bearer {
                req = req.bearer_auth(token);
            }

            let response = req.send().await.map_err(|e| {
                Error::SendRequest(format!(
                    "Could not send {} bytes to {}: {}",
                    payload.len(),
                    url,
                    e
                ))
            })?;

            let status = response.status();
            if !status.is_success() {
                let body = response.bytes().await.map_err(|e| {
                    Error::ReceiveResponse(format!("Could not receive from {}: {}", url, e))
                })?;
                return Err(http_status_error(&url, status, &body));
            }

            let url_for_stream = url.clone();
            let stream = async_stream::try_stream! {
                let mut buffered = Vec::new();
                let mut byte_stream = response.bytes_stream();

                while let Some(chunk) = byte_stream.next().await {
                    let chunk = chunk.map_err(|e| Error::ReceiveResponse(format!("Could not receive from {}: {}", url_for_stream, e)))?;
                    buffered.extend_from_slice(&chunk);

                    while let Some(position) = buffered.iter().position(|byte| *byte == b'\n') {
                        let line = buffered.drain(..=position).collect::<Vec<_>>();
                        let line = &line[..line.len().saturating_sub(1)];
                        if line.is_empty() {
                            continue;
                        }

                        yield decode_call_response::<C>(&url_for_stream, line)?;
                    }
                }

                if !buffered.is_empty() {
                    yield decode_call_response::<C>(&url_for_stream, &buffered)?;
                }
            };

            Ok(Box::pin(stream) as CallResponseStream)
        }
    }
}

fn http_status_error(url: &str, status: StatusCode, body: &[u8]) -> Error {
    let body = String::from_utf8_lossy(body);
    Error::ReceiveResponse(format!(
        "Server {} returned {}: {}",
        url,
        status,
        body.trim()
    ))
}

fn decode_app_error_body(body: &[u8]) -> Payload {
    serde_json::from_slice::<Payload>(body).unwrap_or_else(|_| Payload::new(body))
}

fn decode_call_response<C: UnaryCodec>(url: &str, bytes: &[u8]) -> Result<CallResponse> {
    C::decode::<CallResponse>(bytes).map_err(|e| {
        Error::ReceiveResponse(format!(
            "Could not decode streamed payload from {}: {:?}",
            url, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use axum::{
        Router,
        body::{Body, Bytes},
        http::StatusCode,
        routing::post,
    };
    use futures::{StreamExt, stream};
    use tokio::net::TcpListener;

    use super::HttpTransport;
    use crate::{
        CallResponse, ChunkedTransport, Error, JsonCodec, OneShotTransport, Payload, UnaryCodec,
    };

    #[tokio::test]
    async fn reports_http_error_status_and_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/rpc",
            post(|| async { (StatusCode::BAD_REQUEST, "invalid RPC payload") }),
        );

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let transport = HttpTransport::new(&format!("http://{}", address));
        let error = transport
            .send_payload(Payload::new(b"ping"))
            .await
            .expect_err("request should fail");

        server.abort();

        match error {
            Error::ReceiveResponse(message) => {
                assert!(message.contains("400 Bad Request"));
                assert!(message.contains("invalid RPC payload"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn decodes_chunked_call_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let frames = vec![
            encode_frame(CallResponse::Progress(Payload::new(
                br#"{"currently_at":3}"#,
            ))),
            encode_frame(CallResponse::Success(Payload::new(br#"{"total":4}"#))),
        ];
        let app = Router::new().route(
            "/rpc",
            post(move || {
                let frames = frames.clone();
                async move {
                    (
                        [("content-type", "application/x-ndjson")],
                        Body::from_stream(stream::iter(
                            frames
                                .into_iter()
                                .map(|frame| Ok::<Bytes, Infallible>(Bytes::from(frame))),
                        )),
                    )
                }
            }),
        );

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let transport = HttpTransport::new(&format!("http://{}", address));
        let mut response_stream = <HttpTransport as ChunkedTransport>::send_payload_stream::<
            JsonCodec,
        >(&transport, Payload::new(b"ping"))
        .await
        .expect("request should succeed");

        let first = response_stream
            .next()
            .await
            .expect("progress frame")
            .expect("valid progress frame");
        let second = response_stream
            .next()
            .await
            .expect("success frame")
            .expect("valid success frame");

        server.abort();

        match first {
            CallResponse::Progress(payload) => {
                assert_eq!(payload.get_bytes(), br#"{"currently_at":3}"#);
            }
            other => panic!("unexpected first frame: {other:?}"),
        }

        match second {
            CallResponse::Success(payload) => {
                assert_eq!(payload.get_bytes(), br#"{"total":4}"#);
            }
            other => panic!("unexpected second frame: {other:?}"),
        }
    }

    #[tokio::test]
    async fn decodes_raw_http_422_app_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/rpc",
            post(|| async {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    [("content-type", "application/json")],
                    br#""InvalidCredentials""#.to_vec(),
                )
            }),
        );

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let transport = HttpTransport::new(&format!("http://{}", address));
        let error = transport
            .send_payload(Payload::new(b"ping"))
            .await
            .expect_err("request should fail");

        server.abort();

        match error {
            Error::App(payload) => {
                assert_eq!(payload.get_bytes(), br#""InvalidCredentials""#);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn decodes_legacy_http_422_payload_wrappers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let encoded_payload = JsonCodec::encode(&Payload::new(br#""InvalidCredentials""#))
            .expect("payload should encode")
            .get_bytes_forever();
        let app = Router::new().route(
            "/rpc",
            post(move || {
                let encoded_payload = encoded_payload.clone();
                async move {
                    (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        [("content-type", "application/json")],
                        encoded_payload,
                    )
                }
            }),
        );

        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let transport = HttpTransport::new(&format!("http://{}", address));
        let error = transport
            .send_payload(Payload::new(b"ping"))
            .await
            .expect_err("request should fail");

        server.abort();

        match error {
            Error::App(payload) => {
                assert_eq!(payload.get_bytes(), br#""InvalidCredentials""#);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn encode_frame(frame: CallResponse) -> Vec<u8> {
        let mut bytes = JsonCodec::encode(&frame)
            .expect("frame should encode")
            .get_bytes_forever();
        bytes.push(b'\n');
        bytes
    }
}
