use futures::pin_mut;
use futures::stream::StreamExt;
use progressable_future::{ProgressOutcome, ProgressableFuture, Step, TransportIssue};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    println!("--- Stream example ---");

    let fut = ProgressableFuture::new(|sender| async move {
        for i in 0..=100u8 {
            sender.progress(i);
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, String>("Finished via stream!".to_string())
    });

    let stream = fut.stream();
    pin_mut!(stream); // Required: makes the stream Unpin

    while let Some(step) = stream.next().await {
        match step {
            Step::Progress(p) => println!("Progress: {}%", p),
            Step::Success(val) => println!("✅ Success: {val}"),
            Step::Error(e) => eprintln!("❌ Error: {e}"),
            Step::Transport(TransportIssue::Error(err)) => eprintln!("⚠️ Transport error: {err}"),
            Step::Transport(TransportIssue::Interrupted) => eprintln!("⚠️ Task interrupted"),
        }
    }

    println!("--- Await example ---");

    let fut2 = ProgressableFuture::new(|sender| async move {
        for i in 0..=50u8 {
            sender.progress(i);
            sleep(Duration::from_millis(15)).await;
        }
        Ok::<_, String>("Finished via await!".to_string())
    });

    match fut2.await_result().await {
        ProgressOutcome::Success(val) => println!("✅ Done with await_result: {val}"),
        ProgressOutcome::Error(e) => eprintln!("❌ Error with await_result: {e}"),
        ProgressOutcome::Transport(TransportIssue::Error(err)) => {
            eprintln!("⚠️ Transport error with await_result: {err}")
        }
        ProgressOutcome::Transport(TransportIssue::Interrupted) => {
            eprintln!("⚠️ Task interrupted")
        }
    }
}
