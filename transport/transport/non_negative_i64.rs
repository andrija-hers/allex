use std::sync::atomic::{AtomicI64, Ordering};

pub struct NonNegativeWrappingAtomicI64 {
    inner: AtomicI64,
}

impl NonNegativeWrappingAtomicI64 {
    pub const fn new(value: i64) -> Self {
        assert!(value >= 0);
        Self {
            inner: AtomicI64::new(value),
        }
    }

    pub fn load(&self, ordering: Ordering) -> i64 {
        self.inner.load(ordering)
    }

    pub fn fetch_inc_wrap(&self, ordering: Ordering) -> i64 {
        let mut current = self.inner.load(Ordering::Relaxed);

        debug_assert!(current >= 0);

        loop {
            let next = if current == i64::MAX {
                1
            } else {
                current + 1
            };

            match self.inner.compare_exchange(
                current,
                next,
                ordering,
                Ordering::Relaxed,
            ) {
                Ok(old) => return old,
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::NonNegativeWrappingAtomicI64;

    #[test]
    fn wraps_back_to_one_instead_of_zero() {
        let counter = NonNegativeWrappingAtomicI64::new(i64::MAX);

        assert_eq!(counter.fetch_inc_wrap(Ordering::Relaxed), i64::MAX);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(counter.fetch_inc_wrap(Ordering::Relaxed), 1);
    }
}
