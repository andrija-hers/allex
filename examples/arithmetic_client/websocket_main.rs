use arithmetic_base::*;
use futures::{StreamExt, pin_mut};
use progressable_future::Step;
use transport::{Error, JsonCodec, ProgressOutcome, WebSocketSerialClient};

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    let transport = WebSocketSerialClient::<JsonCodec>::connect("ws://127.0.0.1:3310").await?;
    let a = 5;
    let b = 7;

    match <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::add(&transport, a, b).await {
        ProgressOutcome::Success(r) => println!("{a} + {b} = {}", r.r),
        ProgressOutcome::Error(e)   => eprintln!("add error: {e:?}"),
        ProgressOutcome::Transport(t) => eprintln!("add transport: {t:?}"),
    }

    match <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::mul(&transport, a, b).await {
        ProgressOutcome::Success(r) => println!("{a} * {b} = {}", r.r),
        ProgressOutcome::Error(e)   => eprintln!("mul error: {e:?}"),
        ProgressOutcome::Transport(t) => eprintln!("mul transport: {t:?}"),
    }

    println!("Counting from {a} to {b}");
    let count_future = match <WebSocketSerialClient<JsonCodec> as ArithmeticV1Client<JsonCodec>>::count(&transport, a, b).await {
        Ok(f) => f,
        Err(e) => { eprintln!("could not start count: {e:?}"); return Ok(()); }
    };

    let stream = count_future.stream();
    pin_mut!(stream);
    while let Some(step) = stream.next().await {
        match step {
            Step::Progress(p)  => println!("count progress: {}", p.currently_at),
            Step::Success(r)   => println!("count from {a} to {b} complete: {} steps", r.total),
            Step::Error(e)     => eprintln!("count error: {e:?}"),
            Step::Transport(t) => eprintln!("count transport: {t:?}"),
        }
    }

    Ok(())
}
