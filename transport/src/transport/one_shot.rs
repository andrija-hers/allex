use futures::Stream;
use serde::Serialize;

use crate::*;

pub trait OneShotTransport {
    fn send_payload(&self, payload: Payload) -> impl Future<Output = Result<Payload>> + Send;
}

trait ClientSideOneShotTransportInternal<C: UnaryCodec, S: Serialize> {
    fn send_encoded(&self, thingy: Result<S>) -> impl Future<Output = Result<Payload>> + Send;
}

impl<T: OneShotTransport + Sync, C: UnaryCodec, S: Serialize + Send> ClientSideOneShotTransportInternal<C, S> for T {
    fn send_encoded(
        &self,
        thingy: Result<S>,
    ) -> impl Future<Output = Result<Payload>> + Send  {
        async move {
            match thingy {
                Ok(encoded_thingy) => {
                    match encoded_thingy.encode::<C>() {
                        Ok(encoded) => self.send_payload(encoded).await,
                        Err(e) => Err(e),
                    }
                },
                Err(e) => Err(e)
            }
        }
    }
}

pub trait ClientSideOneShotTransport<C: UnaryCodec, P: Serialize> {
    fn execute_method_call(&self, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send;
    fn execute_module_method_call(&self, module: &str, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send;
    fn execute_relayed_module_method_call(&self, server: &str, module: &str, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send;
}

impl <T: OneShotTransport + Sync, C: UnaryCodec, P: Serialize> ClientSideOneShotTransport<C, P> for T {
    fn execute_method_call(&self, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, MethodCall>>::send_encoded(self, MethodCall::new::<C, P>(method, payload))
    }
    fn execute_module_method_call(&self, module: &str, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, ModuleMethodCall>>::send_encoded(self, ModuleMethodCall::new::<C, P>(module, method, payload))
    }
    fn execute_relayed_module_method_call(&self, server: &str, module: &str, method: &str, payload: P) -> impl Future<Output = Result<Payload>> + Send {
        <T as ClientSideOneShotTransportInternal<C, RelayedModuleMethodCall>>::send_encoded(self, RelayedModuleMethodCall::new::<C, P>(server, module, method, payload))
    }
}


pub trait ServerSideOneShotTransportItem {
    fn get_payload(&self) -> &Payload;

    fn respond_with(
        self,
        payload: Payload,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub trait ServerSideOneShotTransport {
    type Item: ServerSideOneShotTransportItem;
    type ItemStream: Stream<Item = Self::Item>;

    fn get_incoming_stream(&self) -> impl Future<Output = Self::ItemStream>;
}