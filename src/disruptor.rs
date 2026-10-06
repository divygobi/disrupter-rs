use core::ops::BitOr;

use crate::processor::{BatchEventProcessor, EventHandler};
use crate::producer::Producer;
use crate::ring_buffer::{MAX_HANDLERS, RingBuffer};
use crate::sequencer::ProducerType;

/// Wires up handlers on a [`RingBuffer`], then [`start`](Self::start)s it.
///
/// It holds the ring's `&mut` borrow for as long as any processor or producer
/// it creates lives, so the graph can't change once events are flowing:
///
/// ```compile_fail
/// # use lmax_disruptor::{Disruptor, RingBuffer};
/// let mut ring = RingBuffer::<u64, 8>::new(|| 0);
/// let mut disruptor = Disruptor::new(&mut ring);
/// let producer = disruptor.start();
/// disruptor.handle_events_with(|_: &u64, _: i64, _: bool| {}); // moved by `start`
/// ```
///
/// A [`Single`](crate::Single) ring has exactly one producer:
///
/// ```compile_fail
/// # use lmax_disruptor::{Disruptor, RingBuffer};
/// let mut ring = RingBuffer::<u64, 8>::new(|| 0);
/// let producer = Disruptor::new(&mut ring).start();
/// let another = producer.clone(); // only `Multi` producers are `Clone`
/// ```
pub struct Disruptor<'a, E, const N: usize, P: ProducerType> {
    ring: &'a RingBuffer<E, N, P>,
    handler_count: usize,
    /// Handlers with nothing downstream: the producer waits on these.
    leaves: Gate,
}

impl<'a, E, const N: usize, P: ProducerType> Disruptor<'a, E, N, P> {
    /// Starts a new handler graph on `ring`, rewinding all its sequences.
    pub fn new(ring: &'a mut RingBuffer<E, N, P>) -> Self {
        ring.reset();
        Self {
            ring,
            handler_count: 0,
            leaves: Gate::default(),
        }
    }

    /// Adds a handler that sees events as soon as they're published.
    ///
    /// # Panics
    /// If the ring already has [`MAX_HANDLERS`].
    pub fn handle_events_with<H: EventHandler<E>>(
        &mut self,
        handler: H,
    ) -> BatchEventProcessor<'a, E, H, N, P> {
        self.add(Gate::default(), handler)
    }

    /// Selects upstream handlers; handlers added next run after them.
    /// Combine gates with `|` for a diamond.
    pub fn after(&mut self, upstream: Gate) -> After<'_, 'a, E, N, P> {
        After {
            disruptor: self,
            upstream,
        }
    }

    /// Finishes the graph and returns the producer.
    pub fn start(self) -> Producer<'a, E, N, P> {
        Producer::new(self.ring, self.leaves)
    }

    fn add<H: EventHandler<E>>(
        &mut self,
        upstream: Gate,
        handler: H,
    ) -> BatchEventProcessor<'a, E, H, N, P> {
        assert!(self.handler_count < MAX_HANDLERS, "too many handlers");
        let index = self.handler_count;
        self.handler_count += 1;
        self.leaves = self.leaves.without(upstream) | Gate::of(index);
        BatchEventProcessor::new(self.ring, index, upstream, handler)
    }
}

/// Returned by [`Disruptor::after`].
pub struct After<'d, 'a, E, const N: usize, P: ProducerType> {
    disruptor: &'d mut Disruptor<'a, E, N, P>,
    upstream: Gate,
}

impl<'a, E, const N: usize, P: ProducerType> After<'_, 'a, E, N, P> {
    /// Adds a handler that only sees an event once every upstream handler
    /// is done with it.
    pub fn handle_events_with<H: EventHandler<E>>(
        self,
        handler: H,
    ) -> BatchEventProcessor<'a, E, H, N, P> {
        self.disruptor.add(self.upstream, handler)
    }
}

/// A set of handlers, from [`BatchEventProcessor::gate`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Gate(u32);

impl Gate {
    pub(crate) fn of(handler: usize) -> Self {
        Self(1 << handler)
    }

    pub(crate) fn without(self, other: Gate) -> Self {
        Self(self.0 & !other.0)
    }

    /// Indices of the handlers in this set.
    pub(crate) fn handlers(self) -> impl Iterator<Item = usize> {
        let mut bits = self.0;
        core::iter::from_fn(move || {
            let index = (bits != 0).then(|| bits.trailing_zeros() as usize);
            bits &= bits.wrapping_sub(1); // clear lowest set bit
            index
        })
    }
}

impl BitOr for Gate {
    type Output = Gate;

    fn bitor(self, rhs: Gate) -> Gate {
        Gate(self.0 | rhs.0)
    }
}
