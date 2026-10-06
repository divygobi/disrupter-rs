//! A `no_std` port of the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/).
//!
//! A [`RingBuffer`] of preallocated events is shared by producers and a graph
//! of [`EventHandler`]s. Producers write events in place and publish them;
//! every handler sees every event exactly once, in order, in batches.
//!
//! Nothing is allocated and no threads are spawned: you drive each
//! [`BatchEventProcessor`] by calling [`poll`](BatchEventProcessor::poll)
//! from wherever suits the target — a main loop, an ISR, another core.
//!
//! ```
//! use lmax_disruptor::{Disruptor, RingBuffer};
//!
//! let mut ring = RingBuffer::<u64, 8>::new(|| 0);
//! let mut disruptor = Disruptor::new(&mut ring);
//! let mut journal = disruptor.handle_events_with(|e: &u64, _seq: i64, _eob: bool| assert!(*e < 8));
//! let mut logic = disruptor.after(journal.gate()).handle_events_with(|_: &u64, _: i64, _: bool| {});
//! let mut producer = disruptor.start();
//!
//! producer.publish_event(|e, seq| *e = seq as u64);
//! assert_eq!(logic.poll(), 0); // gated: `journal` hasn't seen it yet
//! assert_eq!(journal.poll(), 1);
//! assert_eq!(logic.poll(), 1);
//! ```
//!
//! # Guarantees
//!
//! * A producer never overwrites an event a handler hasn't finished with.
//!   `publish_*` spins until there's room; `try_publish_*` returns
//!   [`InsufficientCapacity`] instead.
//! * Events are visible only once published; a batch becomes visible at once.
//! * Each handler sees every event once, in sequence order. Handlers added
//!   with [`Disruptor::after`] only see an event after their upstream
//!   handlers are done with it.
//! * With [`Multi`], producers can be cloned across cores; each gets
//!   distinct, gap-free sequences.
#![no_std]

mod disruptor;
mod processor;
mod producer;
mod ring_buffer;
mod sequence;
mod sequencer;

pub use disruptor::{After, Disruptor, Gate};
pub use processor::{BatchEventProcessor, EventHandler};
pub use producer::{InsufficientCapacity, Producer};
pub use ring_buffer::{MAX_HANDLERS, RingBuffer};
pub use sequencer::{Multi, ProducerType, Single};
