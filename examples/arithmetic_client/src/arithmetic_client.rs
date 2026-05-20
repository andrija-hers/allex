use transport::{Error, UnaryCodec};
use arithmetic_base::*;
//glue code
pub trait ArithmeticV1Client<C: UnaryCodec> {
    fn add(&self, first: i32, second: i32) -> impl Future<Output = Result<AddResult, Error>> + Send;
    fn mul(&self, first: i32, second: i32) -> impl Future<Output = Result<MulResult, Error>> + Send;
}
//endof glue code
