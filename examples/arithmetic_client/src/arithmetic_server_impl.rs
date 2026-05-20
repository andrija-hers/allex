use arithmetic_base::{ArithmeticV1, AddRequest, AddResult, MulRequest, MulResult, ArithmeticError};

pub struct MyArithmetic;

impl ArithmeticV1 for MyArithmetic {
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError> {
        Ok(AddResult { r: req.a + req.b })
    }
    async fn mul(&self, req: MulRequest) -> Result<MulResult, ArithmeticError> {
        Ok(MulResult { r: req.a * req.b })
    }
}
