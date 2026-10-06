use portable_atomic::{AtomicI64, Ordering};

/// A position in the ring, written by a single owner.
///
/// Writes release and reads acquire, so seeing a sequence also means seeing
/// the event writes that came before it.
pub struct Sequence(AtomicI64);

impl Sequence {
    /// Nothing claimed or processed yet.
    pub const INITIAL: i64 = -1;

    pub const fn new() -> Self {
        Self(AtomicI64::new(Self::INITIAL))
    }

    #[inline]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Release)
    }

    #[inline]
    pub fn compare_and_set(&self, current: i64, new: i64) -> bool {
        self.0
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}
