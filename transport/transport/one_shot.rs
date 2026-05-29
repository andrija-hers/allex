use futures::Stream;
use serde::Serialize;

use crate::*;

/// Minimal request/response transport for unary RPC calls.
pub trait OneShotTransport {
    /// Sends a serialized request payload and resolves with the serialized response payload.
    fn send_payload(&self, payload: Payload) -> impl Future<Output = Result<Payload>> + Send;
}

trait ClientSideOneShotTransportInternal<C: UnaryCodec, S: Serialize> {
    fn send_encoded(&self, thingy: Result<S>) -> impl Future<Output = Result<Payload>> + Send;
}

impl<T: OneShotTransport + Sync, C: UnaryCodec, S: Serialize + Send>
    ClientSideOneShotTransportInternal<C, S> for T
{
    fn send_encoded(&self, thingy: Result<S>) -> impl Future<Output = Result<Payload>> + Send {
        async move {
            match thingy {
                Ok(encoded_thingy) => match encoded_thingy.encode::<C>() {
                    Ok(encoded) => self.send_payload(encoded).await,
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        }
    }
}

/// Convenience client helpers built on top of [`OneShotTransport`].
pub trait ClientSideOneShotTransport<C: UnaryCodec, P: Serialize> {
    /// Executes a bare method call and returns the encoded response payload.
    fn execute_method_call(
        &self,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send;
    /// Executes a module-scoped method call and returns the encoded response payload.
    fn execute_module_method_call(
        &self,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send;
    /// Executes a relayed module call on another named server.
    fn execute_relayed_module_method_call(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send;
}

impl<T: OneShotTransport + Sync, C: UnaryCodec, P: Serialize> ClientSideOneShotTransport<C, P>
    for T
{
    fn execute_method_call(
        &self,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, MethodCall>>::send_encoded(
            self,
            MethodCall::new::<C, P>(method, payload),
        )
    }
    fn execute_module_method_call(
        &self,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, ModuleMethodCall>>::send_encoded(
            self,
            ModuleMethodCall::new::<C, P>(module, method, payload),
        )
    }
    fn execute_relayed_module_method_call(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: P,
    ) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, RelayedModuleMethodCall>>::send_encoded(
            self,
            RelayedModuleMethodCall::new::<C, P>(server, module, method, payload),
        )
    }
}

/// Server-side handle for a single unary transport request.
pub trait ServerSideOneShotTransportItem {
    /// Returns the encoded request payload.
    fn get_payload(&self) -> &Payload;

    /// Completes the request with the provided encoded response payload.
    fn respond_with(self, payload: Payload) -> impl Future<Output = Result<()>> + Send;
}

/// Server-side source of unary transport items.
pub trait ServerSideOneShotTransport {
    /// Item type yielded for each inbound request.
    type Item: ServerSideOneShotTransportItem;
    /// Stream type used to yield inbound requests.
    type ItemStream: Stream<Item = Self::Item>;

    /// Returns the stream of inbound requests handled by this transport.
    fn get_incoming_stream(&self) -> impl Future<Output = Self::ItemStream>;
}
