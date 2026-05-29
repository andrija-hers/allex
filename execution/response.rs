use async_trait::async_trait;
use transport::{Error, Payload, Result, UnaryCodec};

use crate::MethodExecutor;

pub enum Response {
    Success(Payload),
    Progress(Payload),
    Error(Payload),
}

#[async_trait]
pub trait MethodResponder {
    fn get_name(&self) -> &str;

    async fn execute(&self, method: &str, payload: Payload, caller_role: Option<&str>) -> Response;
}

pub trait MethodResponderWithCodec: MethodResponder + Send + Sync {
    type Codec: UnaryCodec;
}

#[async_trait]
impl<T> MethodExecutor for T
where
    T: MethodResponderWithCodec,
{
    fn get_name(&self) -> &str {
        MethodResponder::get_name(self)
    }

    async fn execute(&self, method: &str, payload: Payload, caller_role: Option<&str>) -> Result<Payload> {
        let response = MethodResponder::execute(self, method, payload, caller_role).await;

        match response {
            Response::Success(v) | Response::Progress(v) => Ok(v),

            Response::Error(err) => Err(Error::App(err)),
        }
    }
}
