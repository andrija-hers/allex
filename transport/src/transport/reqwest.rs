use crate::types::*;
use crate::transport::OneShotTransport;

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::SendRequest(format!("Reqwest: {}", value.to_string()))
    }
}
pub struct HttpTransport {
    base_url: String,
}

impl HttpTransport {
    pub fn new(base_url: &str) -> Self {
        Self{base_url: format!("{}/rpc/", base_url)}
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
                .await?;

            let payload = response
                .json::<Payload>()
                .await?;

            Ok(payload)
        }
    }
}