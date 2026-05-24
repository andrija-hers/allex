use async_trait::async_trait;
use transport::{Payload, UnaryCodec, Encodable, Error as TransportError};
use execution::{MethodResponder, MethodResponderWithCodec, Response};

use crate::{ArithmeticV1, AddRequest, MulRequest};

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
