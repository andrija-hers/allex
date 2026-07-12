//! `ProgressableFuture` — an async operation that streams **progress** before it **finishes**.
//!
//! # The idea
//!
//! A normal `Future` yields one value. A [`ProgressableFuture<P, T, E, X>`](ProgressableFuture)
//! yields a sequence of intermediate `P` **progress** updates and then exactly one terminal
//! outcome. That outcome is three-way — [`ProgressOutcome`]: `Success(T)`, `Error(E)` (your domain
//! error), or `Transport(X)` (a plumbing fault). This is the substrate under every streaming
//! `#[rpc_module]` method: the macro turns a method returning `ProgressableFuture<P, T, E>` into a
//! wire stream of progress frames plus one terminal frame, and `transport::ProgressOutcome` is this
//! crate's type re-exported.
//!
//! # Producing one
//!
//! [`ProgressableFuture::new`] takes an `FnOnce(ProgressSender<P>) -> Future<Output = Result<T, E>>`.
//! Call [`ProgressSender::progress`] as often as you like, then return the `Result`. The operation
//! is **spawned onto the runtime immediately**, so `new` must be called inside a tokio context. Use
//! [`ProgressableFuture::new_with_transport`] when you need a concrete transport-error type `X` (it
//! defaults to `Infallible`).
//!
//! # Consuming one — pick your view
//!
//! - [`stream`](ProgressableFuture::stream) → a `Stream<Item = Step<P, T, E, X>>`: each progress
//!   update as [`Step::Progress`], then one terminal `Step`. This is what the server transport maps
//!   onto the wire.
//! - [`await_result`](ProgressableFuture::await_result) → just the terminal [`ProgressOutcome`],
//!   ignoring progress.
//! - [`ProgressOutcome::flatten`] → collapse to `Result<T, E>` (folding a transport issue into `E`
//!   via `From`) so you can `?` it.
//!
//! # Two implementations, one API
//!
//! The default `std` feature gives the ergonomic tokio-backed impl; `no_std + alloc` gives a
//! runtime-agnostic impl exposing the **same** type/enum names, so downstream code stays generic
//! across targets.
//!
//! ```
//! use progressable_future::{ProgressableFuture, Step};
//! use futures::StreamExt;
//!
//! let rt = tokio::runtime::Runtime::new().unwrap();
//! rt.block_on(async {
//!     // progress = u32, success = String, error = String
//!     let pf = ProgressableFuture::<u32, String, String>::new(|tx| async move {
//!         tx.progress(1);
//!         tx.progress(2);
//!         Ok("done".to_string())
//!     });
//!
//!     let steps: Vec<_> = pf.stream().collect().await;
//!     // two Progress steps, then a terminal Success
//!     assert!(matches!(steps[0], Step::Progress(1)));
//!     assert!(matches!(steps.last(), Some(Step::Success(_))));
//! });
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]

// `alloc` usage for heap collections/Box in no_std environments
#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(feature = "std")]
mod imp_std;

#[cfg(feature = "std")]
pub use imp_std::{ProgressOutcome, ProgressSender, ProgressableFuture, Step, TransportIssue};

#[cfg(not(feature = "std"))]
mod imp_alloc;

#[cfg(not(feature = "std"))]
pub use imp_alloc::{ProgressOutcome, ProgressableFuture, Step, TransportIssue};

//
// Note: we publish two implementations:
//  - imp_std : ergonomic tokio-based implementation (default)
//  - imp_alloc: no_std + alloc, runtime-agnostic (requires adapters)
//
// The public API is intentionally the same (types/enum names) so consuming
// code can be generic across targets.
//
