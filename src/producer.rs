use core::fmt;
use core::ops::RangeInclusive;

use crate::disruptor::Gate;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{Multi, ProducerType, Sequencer};

type Claimer<P, const N: usize> = <<P as ProducerType>::Sequencer<N> as Sequencer<N>>::Claimer;

/// Writes events into the ring and publishes them.
///
/// Returned by [`Disruptor::start`](crate::Disruptor::start). There is one
/// per [`Single`](crate::Single) ring; for [`Multi`] it's `Clone`.
pub struct Producer<'a, E, const N: usize, P: ProducerType> {
    ring: &'a RingBuffer<E, N, P>,
    gating: Gate,
    claimer: Claimer<P, N>,
}

/// The slowest handler hasn't released enough slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsufficientCapacity;

impl<'a, E, const N: usize, P: ProducerType> Producer<'a, E, N, P> {
    pub(crate) fn new(ring: &'a RingBuffer<E, N, P>, gating: Gate) -> Self {
        Self {
            ring,
            gating,
            claimer: P::Sequencer::<N>::claimer(),
        }
    }

    /// Claims the next slot, spinning until there's room, lets `write` fill
    /// it in place, then publishes it.
    pub fn publish_event(&mut self, write: impl FnOnce(&mut E, i64)) {
        let range = self.claim(1);
        self.write_one(range, write);
    }

    /// Like [`publish_event`](Self::publish_event), but fails instead of
    /// spinning when the ring is full.
    pub fn try_publish_event(
        &mut self,
        write: impl FnOnce(&mut E, i64),
    ) -> Result<(), InsufficientCapacity> {
        let range = self.try_claim(1)?;
        self.write_one(range, write);
        Ok(())
    }

    /// Claims `n` slots, spinning until there's room, fills each with
    /// `write`, then publishes them as one batch.
    ///
    /// # Panics
    /// If `n` is 0 or more than the capacity.
    pub fn publish_events(&mut self, n: usize, write: impl FnMut(&mut E, i64)) {
        let range = self.claim(n);
        self.write_all(range, write);
    }

    /// Like [`publish_events`](Self::publish_events), but claims nothing and
    /// fails when `n` slots aren't free.
    pub fn try_publish_events(
        &mut self,
        n: usize,
        write: impl FnMut(&mut E, i64),
    ) -> Result<(), InsufficientCapacity> {
        let range = self.try_claim(n)?;
        self.write_all(range, write);
        Ok(())
    }

    fn claim(&mut self, n: usize) -> RangeInclusive<i64> {
        loop {
            match self.try_claim(n) {
                Ok(range) => return range,
                Err(InsufficientCapacity) => core::hint::spin_loop(),
            }
        }
    }

    fn try_claim(&mut self, n: usize) -> Result<RangeInclusive<i64>, InsufficientCapacity> {
        assert!((1..=N).contains(&n), "batch size must be in 1..=capacity");
        let (ring, gating) = (self.ring, self.gating);
        ring.sequencer
            .try_claim(&mut self.claimer, n as i64, || ring.minimum(gating))
            .ok_or(InsufficientCapacity)
    }

    fn write_one(&self, range: RangeInclusive<i64>, write: impl FnOnce(&mut E, i64)) {
        let sequence = *range.start();
        // SAFETY: claimed by us and not yet published.
        write(unsafe { self.ring.get_mut(sequence) }, sequence);
        self.ring.sequencer.publish(range);
    }

    fn write_all(&self, range: RangeInclusive<i64>, mut write: impl FnMut(&mut E, i64)) {
        for sequence in range.clone() {
            // SAFETY: claimed by us and not yet published.
            write(unsafe { self.ring.get_mut(sequence) }, sequence);
        }
        self.ring.sequencer.publish(range);
    }
}

impl<E, const N: usize> Clone for Producer<'_, E, N, Multi> {
    fn clone(&self) -> Self {
        Self::new(self.ring, self.gating)
    }
}

impl fmt::Display for InsufficientCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("insufficient capacity in ring buffer")
    }
}

impl core::error::Error for InsufficientCapacity {}
