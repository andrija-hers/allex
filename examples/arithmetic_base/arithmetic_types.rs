use std::future::Future;
use progressable_future::ProgressableFuture;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AddRequest { pub a: i32, pub b: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct AddResult { pub r: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct MulRequest { pub a: i32, pub b: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct MulResult { pub r: i32 }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountRequest { pub from: i32, pub to: i32 }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountStep { pub currently_at: i32 }
#[derive(Debug, Serialize, Deserialize)]
pub struct StepsMade { pub total: i32 }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArithmeticError {
    Overflow,
    Interrupted,
    Transport(String),
}

pub trait ArithmeticV1 {
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError>;
    fn mul(&self, req: MulRequest) -> impl Future<Output = Result<MulResult, ArithmeticError>> + Send;
    fn count(&self, req: CountRequest) -> ProgressableFuture<CountStep, StepsMade, ArithmeticError>;
}
