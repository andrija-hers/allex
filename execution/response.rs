use async_trait::async_trait;
use transport::{Error, Payload, Result, UnaryCodec};

use crate::MethodExecutor;

/// Server-side response variants produced before transport mapping.
pub enum Response {
    /// Terminal success payload.
    Success(Payload),
    /// Non-terminal progress payload.
    Progress(Payload),
    /// Application-level error payload.
    Error(Payload),
}

/// Turns decoded requests into server-side [`Response`] values.
#[async_trait]
pub trait MethodResponder {
    /// Returns the wire-visible module name handled by this responder.
    fn get_name(&self) -> &str;

    /// Executes a method with the provided serialized payload and optional caller role.
    async fn execute(&self, method: &str, payload: Payload, caller_role: Option<&str>) -> Response;
}

/// A [`MethodResponder`] that also declares which codec its payloads use.
pub trait MethodResponderWithCodec: MethodResponder + Send + Sync {
    /// Codec used to encode and decode application payloads.
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

    async fn execute(
        &self,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload> {
        let response = MethodResponder::execute(self, method, payload, caller_role).await;

        match response {
            Response::Success(v) | Response::Progress(v) => Ok(v),

            Response::Error(err) => Err(Error::App(err)),
        }
    }
}
