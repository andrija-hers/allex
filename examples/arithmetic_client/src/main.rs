use arithmetic_base::*;
mod arithmetic_server_impl;

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
    Ok(())
}