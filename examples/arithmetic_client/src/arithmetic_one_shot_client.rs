use transport::{ClientSideOneShotTransport, Error, UnaryCodec};
use arithmetic_base::{AddRequest, AddResult, MulRequest, MulResult};
use crate::ArithmeticV1Client;
//glue code
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
//endof glue code
