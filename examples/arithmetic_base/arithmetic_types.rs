use std::future::Future;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AddRequest { pub a: i32, pub b: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct AddResult { pub r: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct MulRequest { pub a: i32, pub b: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct MulResult { pub r: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct ArithmeticError;
#[derive(Debug, Serialize, Deserialize)]
pub struct ArithmeticProgress;

pub trait ArithmeticV1 { 
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError>; 
    fn mul(&self, req: MulRequest) -> impl Future<Output = Result<MulResult, ArithmeticError>> + Send; 
}
