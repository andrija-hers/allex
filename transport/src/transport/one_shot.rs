use crate::*;

pub trait OneShotTransport {
    fn send_payload(&self, payload: Payload) -> impl Future<Output = Result<Payload>> + Send;
}

pub trait ClientSideOneShotTransport<C: UnaryCodec> {
    fn execute_method_call(&self, call: MethodCall) -> impl Future<Output = Result<Payload>> + Send;
    //async fn execute_module_method_call(&self, call: ModuleMethodCall) -> Result<Payload>;
    //async fn execute_relayed_module_method_call(&self, call: RelayedModuleMethodCall) -> Result<Payload>;
}

impl<T: OneShotTransport + Sync, C: UnaryCodec> ClientSideOneShotTransport<C> for T {
    fn execute_method_call(
        &self,
        call: MethodCall,
    ) -> impl Future<Output = Result<Payload>> + Send  {
        async move {
            match call.encode::<C>() {
                Ok(encoded) => self.send_payload(encoded).await,
                Err(e) => Err(e),
            }
        }
    }
}
