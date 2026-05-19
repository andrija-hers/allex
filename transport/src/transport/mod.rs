use serde::{Serialize, Deserialize};
use crate::types::*;
use futures::Stream;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame (i64, Payload);

use std::sync::atomic::{Ordering};
mod non_negative_i64;
use non_negative_i64::NonNegativeWrappingAtomicI64;

static FRAMECOUNTER: NonNegativeWrappingAtomicI64 = NonNegativeWrappingAtomicI64::new(0);

impl Frame {
    pub fn new (payload: Payload) -> Self {
        Self(FRAMECOUNTER.fetch_inc_wrap(Ordering::Relaxed), payload)
    }
    pub fn response_to (frame: &Frame, payload: Payload) -> Self {
        Self(-frame.get_id(), payload)
    }
    pub fn get_id(&self) -> i64 {
        self.0
    }
    pub fn get_payload(&self) -> &Payload {
        &self.1
    }
    pub fn is_request(&self) -> bool {
        self.0 > 0
    }
    pub fn is_response(&self) -> bool {
        self.0 < 0
    }
}

pub trait ServerSideTransport {
    type FrameStream: Stream<Item = Frame>;
    fn get_incoming_stream(&self) -> Self::FrameStream;
}


mod one_shot;
pub use one_shot::*;
mod serial;
pub use serial::*;
mod reqwest;
pub use reqwest::*;
mod axum;
pub use axum::*;
