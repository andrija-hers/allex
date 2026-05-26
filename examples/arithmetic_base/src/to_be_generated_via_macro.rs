use transport::{Error, UnaryCodec};
use crate::{AddRequest, AddResult, MulRequest, MulResult};

pub trait ArithmeticV1Client<C: UnaryCodec> {
    fn add(&self, first: i32, second: i32) -> impl Future<Output = Result<AddResult, Error>> + Send;
    fn mul(&self, first: i32, second: i32) -> impl Future<Output = Result<MulResult, Error>> + Send;
}


use transport::{ClientSideOneShotTransport};

impl<T, C> ArithmeticV1Client<C> for T
where
    T: ClientSideOneShotTransport<C, AddRequest> + Sync
        + ClientSideOneShotTransport<C, MulRequest>,
    C: UnaryCodec, {
    async fn add(&self, first: i32, second: i32) -> Result<AddResult, Error> {
        let result = 
        <T as ClientSideOneShotTransport<C, AddRequest>>::execute_module_method_call(
            self, 
            "arithmetic_v1", 
            "add", 
            AddRequest{a: first, b: second})
            .await?.
            decode::<C, AddResult>()?;
            Ok(result)
    } 
    async fn mul(&self, first: i32, second: i32) -> Result<MulResult, Error> {
        let result = 
        <T as ClientSideOneShotTransport<C, AddRequest>>::execute_module_method_call(
            self, 
            "arithmetic_v1", 
            "mul", 
            AddRequest{a: first, b: second})
            .await?.
            decode::<C, MulResult>()?;
            Ok(result)
    }
}

use async_trait::async_trait;
use transport::{Payload, Encodable, Error as TransportError};
use execution::{MethodResponder, MethodResponderWithCodec, Response};

use crate::{ArithmeticV1};

pub struct ArithmeticV1MethodResponder<I, C> {
    inner: I,
    _codec: std::marker::PhantomData<C>,
}

impl<I, C> ArithmeticV1MethodResponder<I, C> {
    pub fn new(inner: I) -> Self {
        Self { inner, _codec: std::marker::PhantomData }
    }
}

#[async_trait]
impl<I, C> MethodResponder for ArithmeticV1MethodResponder<I, C>
where
    I: ArithmeticV1 + Send + Sync,
    C: UnaryCodec + Send + Sync,
{
    fn get_name(&self) -> &str {
        "arithmetic_v1"
    }

    async fn execute(&self, method: &str, payload: Payload) -> Response {
        match method {
            "add" => {
                let req = match payload.decode::<C, AddRequest>() {
                    Ok(r) => r,
                    Err(e) => return Response::Error(e.surely_encode::<C>()),
                };
                match self.inner.add(req) {
                    Ok(result) => match result.encode::<C>() {
                        Ok(encoded) => Response::Success(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                    Err(err) => match err.encode::<C>() {
                        Ok(encoded) => Response::Error(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                }
            }
            "mul" => {
                let req = match payload.decode::<C, MulRequest>() {
                    Ok(r) => r,
                    Err(e) => return Response::Error(e.surely_encode::<C>()),
                };
                match self.inner.mul(req).await {
                    Ok(result) => match result.encode::<C>() {
                        Ok(encoded) => Response::Success(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                    Err(err) => match err.encode::<C>() {
                        Ok(encoded) => Response::Error(encoded),
                        Err(e) => Response::Error(e.surely_encode::<C>()),
                    },
                }
            }
            _ => Response::Error(TransportError::ModuleNotFound.surely_encode::<C>()),
        }
    }
}

impl<I, C> MethodResponderWithCodec for ArithmeticV1MethodResponder<I, C>
where
    I: ArithmeticV1 + Send + Sync,
    C: UnaryCodec + Send + Sync,
{
    type Codec = C;
}
