use arithmetic_base::{
    AddRequest, AddResult, ArithmeticError, ArithmeticV1, CountRequest, CountStep, MulRequest,
    MulResult, StepsMade,
};
use progressable_future::{ProgressSender, ProgressableFuture};
use tokio::time::{Duration, sleep};

pub struct MyArithmetic;

impl ArithmeticV1 for MyArithmetic {
    fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError> {
        Ok(AddResult { r: req.a + req.b })
    }

    async fn mul(&self, req: MulRequest) -> Result<MulResult, ArithmeticError> {
        Ok(MulResult { r: req.a * req.b })
    }

    fn count(
        &self,
        req: CountRequest,
    ) -> ProgressableFuture<CountStep, StepsMade, ArithmeticError> {
        ProgressableFuture::new(move |sender: ProgressSender<CountStep>| async move {
            let step = if req.from <= req.to { 1_i32 } else { -1_i32 };
            let mut current = req.from;
            let mut total = 0_i32;

            loop {
                sender.progress(CountStep {
                    currently_at: current,
                });
                total = total.checked_add(1).ok_or(ArithmeticError::Overflow)?;

                if current == req.to {
                    break;
                }

                sleep(Duration::from_millis(100)).await;
                current = current.checked_add(step).ok_or(ArithmeticError::Overflow)?;
            }

            Ok(StepsMade { total })
        })
    }
}
