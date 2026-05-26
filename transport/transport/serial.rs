use serde::{Serialize, Deserialize};
use futures::Stream;
use crate::{Encodable, MethodCall, ModuleMethodCall, RelayedModuleMethodCall, UnaryCodec, transport_types::{Payload, Result}};
use std::{sync::atomic::Ordering};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame (i64, Payload);

use super::NonNegativeWrappingAtomicI64;
static FRAMECOUNTER: NonNegativeWrappingAtomicI64 = NonNegativeWrappingAtomicI64::new(0);
impl Frame {
    pub fn new (payload: Payload) -> Self {
        Self(FRAMECOUNTER.fetch_inc_wrap(Ordering::Relaxed), payload)
    }
    pub fn response_to (frame: &Frame, payload: Payload) -> Self {
        Self(-frame.get_id(), payload)
    }
    pub fn get_id(&self) -> i64 {
        self.0
    }
    pub fn get_payload(&self) -> &Payload {
        &self.1
    }
    pub fn is_request(&self) -> bool {
        self.0 > 0
    }
    pub fn is_response(&self) -> bool {
        self.0 < 0
    }
}

pub trait ServerSideSerialTransport {
    type FrameStream: Stream<Item = Frame>;
    fn get_incoming_stream(&self) -> Self::FrameStream;
}
pub trait SerialTransport {
    fn send_request(&self, payload: Payload) -> impl Future<Output = Result<i64>> + Send;
}
trait ClientSideSerialTransportInternal<C: UnaryCodec, S: Serialize> {
    fn send_framed (&self, thingy: S) -> impl Future<Output = Result<i64>> + Send;
}

pub trait ClientSideSerialTransport<C: UnaryCodec> {
    fn ask_for_method_execution(&self, call: MethodCall) -> impl Future<Output = Result<i64>> + Send;
    fn ask_for_module_method_execution(&self, call: ModuleMethodCall) -> impl Future<Output = Result<i64>> + Send;
    fn ask_for_relayed_module_method_execution(&self, call: RelayedModuleMethodCall) -> impl Future<Output = Result<i64>> + Send;
}

impl<T: SerialTransport + Sync, C: UnaryCodec, S: Serialize + Send> ClientSideSerialTransportInternal<C, S> for T {
    fn send_framed (&self, thingy: S) -> impl Future<Output = Result<i64>> + Send {
        async move {
            match thingy.encode::<C>() {
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

impl<T: SerialTransport + Sync, C: UnaryCodec> ClientSideSerialTransport<C> for T {
    fn ask_for_method_execution(&self, call: MethodCall) -> impl Future<Output = Result<i64>> + Send {
        <T as ClientSideSerialTransportInternal<C, MethodCall>>::send_framed(self, call)
    }
    fn ask_for_module_method_execution(&self, call: ModuleMethodCall) -> impl Future<Output = Result<i64>> + Send {
        <T as ClientSideSerialTransportInternal<C, ModuleMethodCall>>::send_framed(self, call)
    }
    fn ask_for_relayed_module_method_execution(&self, call: RelayedModuleMethodCall) -> impl Future<Output = Result<i64>> + Send {
        <T as ClientSideSerialTransportInternal<C, RelayedModuleMethodCall>>::send_framed(self, call)
    }
}