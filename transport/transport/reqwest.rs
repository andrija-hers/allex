use crate::transport_types::*;
use crate::transport::OneShotTransport;
use reqwest::StatusCode;

pub struct HttpTransport {
    base_url: String,
}

impl HttpTransport {
    pub fn new(base_url: &str) -> Self {
        Self{base_url: format!("{}/rpc", base_url)}
    }
}

impl OneShotTransport for HttpTransport {
    fn send_payload(
        &self,
        payload: Payload,
    ) -> impl Future<Output = Result<Payload>> + Send {
        let url = self.base_url.clone();

        async move {
            let client = reqwest::Client::new();

            let response = client
                .post(&url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| Error::SendRequest(format!("Could not send {} bytes to {}: {}", payload.len(), url, e.to_string())))?;

            let status = response.status();
            let body = response
                .bytes()
                .await
                .map_err(|e| Error::ReceiveResponse(format!("Could not receive from {}: {}", url, e)))?;

            if !status.is_success() {
                return Err(http_status_error(&url, status, &body));
            }

            let payload = serde_json::from_slice::<Payload>(&body)
                .map_err(|e| Error::ReceiveResponse(format!("Could not decode payload from {}: {}", url, e)))?;

            Ok(payload)
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

#[cfg(test)]
mod tests {
    use axum::{Router, http::StatusCode, routing::post};
    use tokio::net::TcpListener;

    use super::HttpTransport;
    use crate::{Error, OneShotTransport, Payload};

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
}
