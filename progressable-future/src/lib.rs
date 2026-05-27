//! ProgressableFuture: a Future that emits progress updates with progress events.
//!
//! Supports std (tokio + futures) and no_std + alloc environments.

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
