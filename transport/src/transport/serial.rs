use crate::{Encodable, Frame, MethodCall, ModuleMethodCall, RelayedModuleMethodCall, UnaryCodec, types::{Payload, Result}};

pub trait SerialTransport {
    fn send_request(&self, payload: Payload) -> impl Future<Output = Result<i64>> + Send;
}

pub trait ClientSideSerialTransport<C: UnaryCodec> {
    fn ask_for_method_execution(&self, call: MethodCall) -> impl Future<Output = Result<i64>> + Send;
    //fn ask_for_module_method_execution(&self, call: ModuleMethodCall) -> impl Future<Output = Result<i64>> + Send;
    //fn ask_for_relayed_module_method_execution(&self, call: RelayedModuleMethodCall) -> impl Future<Output = Result<i64>> + Send;
}

impl<T: SerialTransport + Sync, C: UnaryCodec> ClientSideSerialTransport<C> for T {
    fn ask_for_method_execution(&self, call: MethodCall) -> impl Future<Output = Result<i64>> + Send {
        async move {
            match call.encode::<C>() {
                Ok(encoded_call) => {
                    let frame = Frame::new(encoded_call);
                    match frame.encode::<C>() {
                        Ok(encoded_frame) => self.send_request(encoded_frame).await,
                        Err(e) => Err(e)
                    }
                },
                Err(e) => Err(e)
            }
        }
    }
}