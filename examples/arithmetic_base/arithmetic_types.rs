use progressable_future::ProgressableFuture;
use rpc_macro::rpc_module;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// Request payload for adding two integers.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddRequest {
    /// Left operand.
    pub a: i32,
    /// Right operand.
    pub b: i32,
}
/// Result payload for integer addition.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddResult {
    /// Computed sum.
    pub r: i32,
}
/// Request payload for multiplying two integers.
#[derive(Debug, Serialize, Deserialize)]
pub struct MulRequest {
    /// Left operand.
    pub a: i32,
    /// Right operand.
    pub b: i32,
}
/// Result payload for integer multiplication.
#[derive(Debug, Serialize, Deserialize)]
pub struct MulResult {
    /// Computed product.
    pub r: i32,
}
/// Request payload for counting from one value to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountRequest {
    /// Inclusive starting value.
    pub from: i32,
    /// Inclusive ending value.
    pub to: i32,
}
/// Progress payload emitted while counting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountStep {
    /// Current value reached by the counter.
    pub currently_at: i32,
}
/// Final payload emitted after counting completes.
#[derive(Debug, Serialize, Deserialize)]
pub struct StepsMade {
    /// Total number of emitted steps.
    pub total: i32,
}
/// Errors surfaced by the arithmetic example service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArithmeticError {
    /// The requested arithmetic operation overflowed.
    Overflow,
    /// The long-running operation was interrupted before completion.
    Interrupted,
    /// A transport-level failure was folded into the example error type.
    Transport(String),
}

/// Example arithmetic RPC surface used throughout the workspace examples.
#[rpc_module]
pub trait ArithmeticV1 {
    /// Adds two numbers and returns the resulting sum.
    #[public]
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError>;
    /// Multiplies two numbers; only callers with the `user` role may invoke it.
    #[allowed_roles("user")]
    fn mul(
        &self,
        req: MulRequest,
    ) -> impl Future<Output = Result<MulResult, ArithmeticError>> + Send;
    /// Counts from `from` to `to`, streaming intermediate progress to `admin` callers.
    #[allowed_roles("admin")]
    fn count(&self, req: CountRequest)
    -> ProgressableFuture<CountStep, StepsMade, ArithmeticError>;
}
