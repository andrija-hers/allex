use arithmetic_base::*;
use futures::{StreamExt, pin_mut};
use progressable_future::Step;
use transport::{HttpTransport, JsonCodec, ProgressOutcome};

#[tokio::main]
pub async fn main() {
    let http_transport = HttpTransport::new("http://127.0.0.1:3300");
    let a = 5;
    let b = 7;

    match <HttpTransport as ArithmeticV1Client<JsonCodec>>::add(
        &http_transport,
        AddRequest { a, b },
    )
    .await
    {
        ProgressOutcome::Success(r) => println!("{a} + {b} = {}", r.r),
        ProgressOutcome::Error(e) => eprintln!("add error: {e:?}"),
        ProgressOutcome::Transport(t) => eprintln!("add transport: {t:?}"),
    }

    match <HttpTransport as ArithmeticV1Client<JsonCodec>>::mul(
        &http_transport,
        MulRequest { a, b },
    )
    .await
    {
        ProgressOutcome::Success(r) => println!("{a} * {b} = {}", r.r),
        ProgressOutcome::Error(e) => eprintln!("mul error: {e:?}"),
        ProgressOutcome::Transport(t) => eprintln!("mul transport: {t:?}"),
    }

    println!("Counting from {a} to {b}");
    let count_future = match <HttpTransport as ArithmeticV1Client<JsonCodec>>::count(
        &http_transport,
        CountRequest { from: a, to: b },
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("could not start count: {e:?}");
            return;
        }
    };

    let stream = count_future.stream();
    pin_mut!(stream);
    while let Some(step) = stream.next().await {
        match step {
            Step::Progress(p) => println!("count progress: {}", p.currently_at),
            Step::Success(r) => println!("count from {a} to {b} complete: {} steps", r.total),
            Step::Error(e) => eprintln!("count error: {e:?}"),
            Step::Transport(t) => eprintln!("count transport: {t:?}"),
        }
    }
}
