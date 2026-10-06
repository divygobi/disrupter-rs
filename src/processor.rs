use crate::disruptor::Gate;
use crate::ring_buffer::RingBuffer;
use crate::sequencer::{ProducerType, Sequencer};

/// Consumes events. Implemented for any `FnMut(&E, i64, bool)`.
pub trait EventHandler<E> {
    /// Called once per event, in sequence order. `end_of_batch` marks the
    /// last event currently available — a good point to flush.
    fn on_event(&mut self, event: &E, sequence: i64, end_of_batch: bool);
}

impl<E, F: FnMut(&E, i64, bool)> EventHandler<E> for F {
    fn on_event(&mut self, event: &E, sequence: i64, end_of_batch: bool) {
        self(event, sequence, end_of_batch)
    }
}

/// Feeds one [`EventHandler`] from the ring, in batches.
///
/// It does nothing on its own: call [`poll`](Self::poll) from a loop, an
/// ISR or another core, and wait between polls however suits the target.
pub struct BatchEventProcessor<'a, E, H, const N: usize, P: ProducerType> {
    ring: &'a RingBuffer<E, N, P>,
    index: usize,
    upstream: Gate,
    handler: H,
}

impl<'a, E, H: EventHandler<E>, const N: usize, P: ProducerType>
    BatchEventProcessor<'a, E, H, N, P>
{
    pub(crate) fn new(
        ring: &'a RingBuffer<E, N, P>,
        index: usize,
        upstream: Gate,
        handler: H,
    ) -> Self {
        Self {
            ring,
            index,
            upstream,
            handler,
        }
    }

    /// Hands every available event to the handler as one batch and
    /// returns how many there were. Never blocks.
    pub fn poll(&mut self) -> usize {
        let next = self.sequence() + 1;
        let limit = self.ring.minimum(self.upstream);
        let Some(end) = self.ring.sequencer.available(next, limit) else {
            return 0;
        };
        for sequence in next..=end {
            // SAFETY: published, past our upstream handlers, and held until
            // our sequence moves past it below.
            let event = unsafe { self.ring.get(sequence) };
            self.handler.on_event(event, sequence, sequence == end);
        }
        self.ring.handlers[self.index].set(end);
        (end - next + 1) as usize
    }

    /// Use with [`Disruptor::after`](crate::Disruptor::after) to run
    /// handlers after this one.
    pub fn gate(&self) -> Gate {
        Gate::of(self.index)
    }

    /// The last sequence this handler finished.
    pub fn sequence(&self) -> i64 {
        self.ring.handlers[self.index].get()
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn into_handler(self) -> H {
        self.handler
    }
}
