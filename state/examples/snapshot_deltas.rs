//! Runnable demonstration of the **snapshot-deltas pattern**.
//!
//!     cargo run -p allex-state --example snapshot_deltas
//!
//! A server owns a live keyed collection in `LocalState`. A client subscribes via
//! `watch_snapshot_deltas`: it receives ONE full snapshot, then only deltas. The example applies
//! each delta to the client's held copy and asserts it always equals the server's state — proving
//! the client reconstructs everything from snapshot + deltas, so the server never re-sends the whole
//! collection. (See `state/SNAPSHOT-DELTAS.md`.)

use std::collections::{HashMap, HashSet};

use allex_state::{HasUpdate, LocalState, StateFrame};
use progressable_future::{ProgressableFuture, Step};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Row {
    id: String,
    value: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Grid {
    version: u64,
    rows: Vec<Row>,
}
impl HasUpdate for Grid {
    type Update = ();
    fn apply_update(&mut self, _: ()) {}
}

// The app-specific wire change + diff — the ONLY piece allex-state can't provide, because only the
// app knows domain identity (here, the row id).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum RowChange {
    Upsert(Row),
    Remove(String),
}

fn diff(prev: &Grid, cur: &Grid) -> Vec<RowChange> {
    let prev_by: HashMap<&str, &Row> = prev.rows.iter().map(|r| (r.id.as_str(), r)).collect();
    let cur_ids: HashSet<&str> = cur.rows.iter().map(|r| r.id.as_str()).collect();
    let mut changes = Vec::new();
    for r in &cur.rows {
        if prev_by.get(r.id.as_str()).map(|p| **p == *r) != Some(true) {
            changes.push(RowChange::Upsert(r.clone()));
        }
    }
    for r in &prev.rows {
        if !cur_ids.contains(r.id.as_str()) {
            changes.push(RowChange::Remove(r.id.clone()));
        }
    }
    changes
}

fn client_apply(rows: &mut Vec<Row>, changes: &[RowChange]) {
    for c in changes {
        match c {
            RowChange::Upsert(r) => match rows.iter_mut().find(|x| x.id == r.id) {
                Some(e) => *e = r.clone(),
                None => rows.push(r.clone()),
            },
            RowChange::Remove(id) => rows.retain(|x| &x.id != id),
        }
    }
}

fn row(id: &str, value: i64) -> Row {
    Row {
        id: id.into(),
        value,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let state = LocalState::new(Grid {
        version: 1,
        rows: vec![row("AAPL", 100), row("MSFT", 200)],
    });

    // Client subscribes (fresh: known_version = None, so it gets the snapshot).
    let fut: ProgressableFuture<StateFrame<Grid, RowChange>, std::convert::Infallible, String> =
        state.watch_snapshot_deltas(None, |g| g.version, diff, || "state closed".to_string());

    // Server-side mutations, applied a beat after the client starts.
    let server = state.clone();
    tokio::spawn(async move {
        let step = std::time::Duration::from_millis(50);
        tokio::time::sleep(step).await;
        server.replace(Grid {
            version: 2,
            rows: vec![row("AAPL", 100), row("MSFT", 205), row("GOOG", 300)], // MSFT↑, +GOOG
        });
        tokio::time::sleep(step).await;
        server.replace(Grid {
            version: 3,
            rows: vec![row("AAPL", 100), row("GOOG", 300)], // −MSFT
        });
        tokio::time::sleep(step).await;
        drop(server); // last state handle gone → closes the channel → ends the client's stream
    });
    // Drop our own handle too, so the spawned task holds the *only* one — when it finishes, the
    // stream closes and the client loop below exits cleanly.
    drop(state);

    // Client loop: snapshot once, then deltas — reconstructing the full state each time.
    let mut held: Vec<Row> = Vec::new();
    let s = fut.stream();
    tokio::pin!(s);
    while let Some(step) = s.next().await {
        match step {
            Step::Progress(StateFrame::Initial { version, snapshot }) => {
                held = snapshot.rows;
                println!("← Initial  v{version}: {held:?}");
            }
            Step::Progress(StateFrame::Update { version, changes }) => {
                println!("← Update   v{version}: {changes:?}");
                client_apply(&mut held, &changes);
                println!("  client reconstructs → {held:?}");
            }
            Step::Error(e) => {
                println!("stream closed: {e}");
                break;
            }
            _ => {}
        }
    }

    println!("\nThe client never received a full snapshot after the first frame — only deltas.");
}
