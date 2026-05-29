use transport::Error;
mod arithmetic_one_shot_axum_server;
use arithmetic_one_shot_axum_server::run_axum_one_shot;
mod arithmetic_server_impl;
use arithmetic_server_impl::MyArithmetic;

#[tokio::main]
async fn main() -> Result<(), Error> {
    run_axum_one_shot().await;
    Ok(())
}
