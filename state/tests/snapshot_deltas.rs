//! The **snapshot-deltas pattern** (see `state/lib.rs` docs and `state/SNAPSHOT-DELTAS.md`):
//! a client gets one full snapshot, then only deltas. These tests pin the contract:
//! - the first frame is the snapshot, every later frame is a delta;
//! - applying the deltas to the initial snapshot reconstructs the live state exactly;
//! - empty diffs are suppressed (no idle frames);
//! - `known_version` lets a reconnecting client skip the initial snapshot.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use allex_state::{HasUpdate, LocalState, StateFrame};
use progressable_future::{ProgressableFuture, Step};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::StreamExt;

// ---- a tiny keyed collection state ----

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
// We drive changes with `replace`, so the in-process Update is unused here.
impl HasUpdate for Grid {
    type Update = ();
    fn apply_update(&mut self, _: ()) {}
}

fn row(id: &str, value: i64) -> Row {
    Row {
        id: id.into(),
        value,
    }
}
fn grid(version: u64, rows: &[Row]) -> Grid {
    Grid {
        version,
        rows: rows.to_vec(),
    }
}

// ---- the app-specific wire change + diff (the only piece allex-state can't provide) ----

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum RowChange {
    Upsert(Row),
    Remove(String),
}

/// Net changes from `prev` to `cur`, keyed by row id.
fn diff(prev: &Grid, cur: &Grid) -> Vec<RowChange> {
    let prev_by: HashMap<&str, &Row> = prev.rows.iter().map(|r| (r.id.as_str(), r)).collect();
    let cur_ids: HashSet<&str> = cur.rows.iter().map(|r| r.id.as_str()).collect();
    let mut changes = Vec::new();
    for r in &cur.rows {
        match prev_by.get(r.id.as_str()) {
            Some(p) if **p == *r => {} // unchanged
            _ => changes.push(RowChange::Upsert(r.clone())),
        }
    }
    for r in &prev.rows {
        if !cur_ids.contains(r.id.as_str()) {
            changes.push(RowChange::Remove(r.id.clone()));
        }
    }
    changes
}

/// What a client does with a delta: apply it to the snapshot it already holds.
fn client_apply(rows: &mut Vec<Row>, changes: &[RowChange]) {
    for c in changes {
        match c {
            RowChange::Upsert(r) => match rows.iter_mut().find(|x| x.id == r.id) {
                Some(existing) => *existing = r.clone(),
                None => rows.push(r.clone()),
            },
            RowChange::Remove(id) => rows.retain(|x| &x.id != id),
        }
    }
}

type Frame = StateFrame<Grid, RowChange>;

/// Drain a watch_snapshot_deltas stream's progress frames into a channel (a task owns the pinned
/// stream; the test pulls frames with a timeout).
fn drain(
    fut: ProgressableFuture<Frame, std::convert::Infallible, String>,
) -> UnboundedReceiver<Frame> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let s = fut.stream();
        tokio::pin!(s);
        while let Some(step) = s.next().await {
            if let Step::Progress(f) = step {
                if tx.send(f).is_err() {
                    break;
                }
            }
        }
    });
    rx
}

async fn pull(rx: &mut UnboundedReceiver<Frame>) -> Option<Frame> {
    tokio::time::timeout(Duration::from_millis(300), rx.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn snapshot_then_deltas_reconstruct_full_state() {
    let state = LocalState::new(grid(1, &[row("A", 1), row("B", 2)]));
    let fut: ProgressableFuture<Frame, std::convert::Infallible, String> =
        state.watch_snapshot_deltas(None, |g| g.version, diff, || "closed".to_string());
    let mut rx = drain(fut);

    // Frame 1: the full snapshot.
    let mut client = match pull(&mut rx).await.expect("initial frame") {
        StateFrame::Initial { version, snapshot } => {
            assert_eq!(version, 1);
            assert_eq!(snapshot.rows, vec![row("A", 1), row("B", 2)]);
            snapshot.rows
        }
        other => panic!("first frame must be Initial, got {other:?}"),
    };

    // Change: B 2->5, add C=9. Expect a delta of two upserts, A untouched.
    state.replace(grid(2, &[row("A", 1), row("B", 5), row("C", 9)]));
    match pull(&mut rx).await.expect("update frame") {
        StateFrame::Update { version, changes } => {
            assert_eq!(version, 2);
            assert_eq!(
                changes,
                vec![
                    RowChange::Upsert(row("B", 5)),
                    RowChange::Upsert(row("C", 9))
                ]
            );
            client_apply(&mut client, &changes);
        }
        other => panic!("expected Update, got {other:?}"),
    }
    assert_eq!(
        client,
        vec![row("A", 1), row("B", 5), row("C", 9)],
        "deltas reconstruct state"
    );

    // Remove B.
    state.replace(grid(3, &[row("A", 1), row("C", 9)]));
    match pull(&mut rx).await.expect("remove frame") {
        StateFrame::Update { version, changes } => {
            assert_eq!(version, 3);
            assert_eq!(changes, vec![RowChange::Remove("B".into())]);
            client_apply(&mut client, &changes);
        }
        other => panic!("expected Update, got {other:?}"),
    }
    assert_eq!(client, vec![row("A", 1), row("C", 9)]);

    // No-op: version bumps but rows are identical → empty diff → NO frame.
    state.replace(grid(4, &[row("A", 1), row("C", 9)]));
    assert!(
        pull(&mut rx).await.is_none(),
        "a change with no semantic delta must not produce a frame"
    );
}

#[tokio::test]
async fn known_version_skips_initial_snapshot() {
    let state = LocalState::new(grid(7, &[row("A", 1)]));
    // A reconnecting client that already holds version 7.
    let fut: ProgressableFuture<Frame, std::convert::Infallible, String> =
        state.watch_snapshot_deltas(Some(7), |g| g.version, diff, || "closed".to_string());
    let mut rx = drain(fut);

    // No initial snapshot — the client is already current.
    assert!(
        pull(&mut rx).await.is_none(),
        "matching known_version skips the snapshot"
    );

    // The next change arrives as a delta, not a snapshot.
    state.replace(grid(8, &[row("A", 2)]));
    match pull(&mut rx).await.expect("update frame") {
        StateFrame::Update { version, changes } => {
            assert_eq!(version, 8);
            assert_eq!(changes, vec![RowChange::Upsert(row("A", 2))]);
        }
        other => panic!("expected Update after reconnect, got {other:?}"),
    }
}
