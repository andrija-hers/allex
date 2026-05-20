use arithmetic_base::*;
mod arithmetic_client;
use arithmetic_client::*;
mod arithmetic_one_shot_client;
mod arithmetic_server_impl;
mod arithmetic_one_shot_axum_server;

use transport::{Error, HttpTransport, JsonCodec};



#[tokio::main]
pub async fn main () -> Result<(), Error>{
    let http_transport = HttpTransport::new("https://dateright.com");
    let add_result = 
    <HttpTransport as ArithmeticV1Client<JsonCodec>>::add(
        &http_transport, 
        5, 
        7).await?;
    Ok(())
}