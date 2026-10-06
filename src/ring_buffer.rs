use core::cell::UnsafeCell;

use crate::disruptor::Gate;
use crate::sequence::Sequence;
use crate::sequencer::{ProducerType, Sequencer, Single};

/// Maximum number of handlers per ring buffer.
pub const MAX_HANDLERS: usize = 16;

/// `N` preallocated events, reused in place, plus the sequences that
/// coordinate access to them.
///
/// It's owned by you — in a `static`, or on a stack shared with scoped
/// threads — and borrowed by [`Disruptor`](crate::Disruptor) to wire up
/// producers and handlers.
pub struct RingBuffer<E, const N: usize, P: ProducerType = Single> {
    events: [UnsafeCell<E>; N],
    pub(crate) sequencer: P::Sequencer<N>,
    pub(crate) handlers: [Sequence; MAX_HANDLERS],
}

// SAFETY: an event is written only by the producer that claimed it, and read
// only after it's published and before every gating handler has moved past it.
unsafe impl<E: Send + Sync, const N: usize, P: ProducerType> Sync for RingBuffer<E, N, P> {}

impl<E, const N: usize, P: ProducerType> RingBuffer<E, N, P> {
    const POWER_OF_TWO: () = assert!(N.is_power_of_two(), "capacity must be a power of two");

    /// Fills every slot using `factory`.
    pub fn new(mut factory: impl FnMut() -> E) -> Self {
        let () = Self::POWER_OF_TWO;
        Self {
            events: core::array::from_fn(|_| UnsafeCell::new(factory())),
            sequencer: P::Sequencer::new(),
            handlers: [const { Sequence::new() }; MAX_HANDLERS],
        }
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    /// Rewinds every sequence so a new graph can be wired up.
    pub(crate) fn reset(&mut self) {
        self.sequencer = P::Sequencer::new();
        self.handlers = [const { Sequence::new() }; MAX_HANDLERS];
    }

    /// The lowest sequence finished by the handlers in `gate`, or `i64::MAX`
    /// if it's empty.
    pub(crate) fn minimum(&self, gate: Gate) -> i64 {
        gate.handlers()
            .map(|i| self.handlers[i].get())
            .min()
            .unwrap_or(i64::MAX)
    }

    /// # Safety
    /// `sequence` must be published and not yet released by the caller.
    pub(crate) unsafe fn get(&self, sequence: i64) -> &E {
        unsafe { &*self.slot(sequence) }
    }

    /// # Safety
    /// `sequence` must be claimed by the caller and not yet published.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_mut(&self, sequence: i64) -> &mut E {
        unsafe { &mut *self.slot(sequence) }
    }

    fn slot(&self, sequence: i64) -> *mut E {
        self.events[sequence as usize & (N - 1)].get()
    }
}
