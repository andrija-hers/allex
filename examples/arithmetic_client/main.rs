use arithmetic_base::*;
use futures::{StreamExt, pin_mut};
use progressable_future::Step;

use transport::{Error, HttpTransport, JsonCodec};

#[tokio::main]
pub async fn main () -> Result<(), Error>{
    let http_transport = HttpTransport::new("http://127.0.0.1:3300");
    let a = 5;
    let b = 7;
    let add_result = 
    <HttpTransport as ArithmeticV1Client<JsonCodec>>::add(
        &http_transport, 
        a, 
        b).await?;
    println!("{} + {} = {:?}", a, b, add_result.r);
    let mul_result =
        <HttpTransport as ArithmeticV1Client<JsonCodec>>::mul(
        &http_transport, 
        a, 
        b).await?;
    println!("{} * {} = {:?}", a, b, mul_result.r);

    println!("Counting from {} to {}", a, b);
    let count_future =
        <HttpTransport as ArithmeticV1Client<JsonCodec>>::count(
            &http_transport,
            a,
            b,
        )
        .await?;
    let count_stream = count_future.stream();
    pin_mut!(count_stream);
    while let Some(step) = count_stream.next().await {
        match step {
            Step::Progress(progress) => println!("count progress: {}", progress.currently_at),
            Step::Success(result) => println!("count complete: {} steps", result.total),
            Step::Error(error) => println!("count failed: {:?}", error),
            Step::MissedSome => println!("count missed some progress"),
            Step::Canceled => println!("count canceled"),
            Step::Dropped => println!("count dropped"),
        }
    }
    Ok(())
}
