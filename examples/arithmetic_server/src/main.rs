use transport::{Error};
mod arithmetic_one_shot_axum_server;
use arithmetic_one_shot_axum_server::run_axum_one_shot;


#[tokio::main]
async fn main () -> Result<(), Error> {
    run_axum_one_shot().await;
    Ok(())
}