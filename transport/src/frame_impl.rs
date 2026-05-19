use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};
use crate::types::*;

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Frame {
    pub fn new (payload: Payload) -> Self {
        Self(COUNTER.fetch_add(1, Ordering::Relaxed), payload)
    }
}