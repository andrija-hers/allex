//! Transport-neutral payload types, codecs, and concrete client/server transports.

// The client/server transports implement trait methods whose signatures are `-> impl Future + Send`
// (so they can't be written as `async fn`), and use some deliberately-nested `Arc<Mutex<HashMap<..>>>`
// pending-call maps — both are intentional; silence the corresponding style lints crate-wide.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

pub use progressable_future::{ProgressOutcome, TransportIssue};
pub use tracing;
mod transport_types;
pub use transport_types::*;
mod transport;
pub use transport::*;
mod codecs;
pub use codecs::*;
