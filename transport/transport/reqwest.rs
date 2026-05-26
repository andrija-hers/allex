use crate::transport_types::*;
use crate::transport::OneShotTransport;

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

            let payload = response
                .json::<Payload>()
                .await
                .map_err(|e| Error::ReceiveResponse(format!("Could not recieve from {}: {}", url, e.to_string())))?;

            Ok(payload)
        }
    }
}