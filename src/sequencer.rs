//! How producers claim and publish sequences: [`Single`] or [`Multi`].

use core::ops::RangeInclusive;

use portable_atomic::{AtomicI64, Ordering};

use crate::sequence::Sequence;

/// Whether a [`RingBuffer`](crate::RingBuffer) has one producer or many.
pub trait ProducerType: sealed::Sealed + 'static {
    #[doc(hidden)]
    type Sequencer<const N: usize>: Sequencer<N>;
}

/// One producer. Claims are uncontended.
pub enum Single {}

/// Many producers: [`Producer`](crate::Producer) is `Clone`.
pub enum Multi {}

impl ProducerType for Single {
    type Sequencer<const N: usize> = SingleSequencer;
}

impl ProducerType for Multi {
    type Sequencer<const N: usize> = MultiSequencer<N>;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Single {}
    impl Sealed for super::Multi {}
}

#[doc(hidden)]
pub trait Sequencer<const N: usize>: Sync {
    /// Per-producer claim state.
    type Claimer: Send;

    fn new() -> Self;
    fn claimer() -> Self::Claimer;

    /// Claims `n` sequences, or `None` if that would overrun the slowest
    /// gating sequence returned by `gating`.
    fn try_claim(
        &self,
        claimer: &mut Self::Claimer,
        n: i64,
        gating: impl Fn() -> i64,
    ) -> Option<RangeInclusive<i64>>;

    fn publish(&self, range: RangeInclusive<i64>);

    /// The last published sequence in `next..=limit`, if any.
    fn available(&self, next: i64, limit: i64) -> Option<i64>;
}

/// The cursor is the last published sequence.
#[doc(hidden)]
pub struct SingleSequencer {
    cursor: Sequence,
}

#[doc(hidden)]
pub struct SingleClaimer {
    next: i64,
    cached_gating: i64,
}

impl<const N: usize> Sequencer<N> for SingleSequencer {
    type Claimer = SingleClaimer;

    fn new() -> Self {
        Self {
            cursor: Sequence::new(),
        }
    }

    fn claimer() -> SingleClaimer {
        SingleClaimer {
            next: Sequence::INITIAL,
            cached_gating: Sequence::INITIAL,
        }
    }

    fn try_claim(
        &self,
        c: &mut SingleClaimer,
        n: i64,
        gating: impl Fn() -> i64,
    ) -> Option<RangeInclusive<i64>> {
        let end = c.next + n;
        if !has_capacity::<N>(end, &mut c.cached_gating, gating) {
            return None;
        }
        c.next = end;
        Some(end - n + 1..=end)
    }

    fn publish(&self, range: RangeInclusive<i64>) {
        self.cursor.set(*range.end());
    }

    fn available(&self, next: i64, limit: i64) -> Option<i64> {
        let end = self.cursor.get().min(limit);
        (end >= next).then_some(end)
    }
}

/// The cursor is the last *claimed* sequence; each slot records the
/// sequence last published into it.
#[doc(hidden)]
pub struct MultiSequencer<const N: usize> {
    cursor: Sequence,
    published: [AtomicI64; N],
}

#[doc(hidden)]
pub struct MultiClaimer {
    cached_gating: i64,
}

impl<const N: usize> MultiSequencer<N> {
    fn slot(&self, sequence: i64) -> &AtomicI64 {
        &self.published[sequence as usize & (N - 1)]
    }
}

impl<const N: usize> Sequencer<N> for MultiSequencer<N> {
    type Claimer = MultiClaimer;

    fn new() -> Self {
        Self {
            cursor: Sequence::new(),
            published: [const { AtomicI64::new(Sequence::INITIAL) }; N],
        }
    }

    fn claimer() -> MultiClaimer {
        MultiClaimer {
            cached_gating: Sequence::INITIAL,
        }
    }

    fn try_claim(
        &self,
        c: &mut MultiClaimer,
        n: i64,
        gating: impl Fn() -> i64,
    ) -> Option<RangeInclusive<i64>> {
        loop {
            let current = self.cursor.get();
            if !has_capacity::<N>(current + n, &mut c.cached_gating, &gating) {
                return None;
            }
            if self.cursor.compare_and_set(current, current + n) {
                return Some(current + 1..=current + n);
            }
        }
    }

    fn publish(&self, range: RangeInclusive<i64>) {
        // Last to first: readers scan from the start, so the whole batch
        // appears when its first slot is marked.
        for sequence in range.rev() {
            self.slot(sequence).store(sequence, Ordering::Release);
        }
    }

    fn available(&self, next: i64, limit: i64) -> Option<i64> {
        // Claimed isn't published: scan for the published prefix.
        (next..=self.cursor.get().min(limit))
            .take_while(|&s| self.slot(s).load(Ordering::Acquire) == s)
            .last()
    }
}

/// Whether claiming up to `end` keeps clear of the slowest gating sequence.
/// Only re-reads the gating sequences when the cached value says no.
fn has_capacity<const N: usize>(end: i64, cached: &mut i64, gating: impl Fn() -> i64) -> bool {
    let wrap_point = end - N as i64;
    if wrap_point > *cached {
        *cached = gating();
    }
    wrap_point <= *cached
}
