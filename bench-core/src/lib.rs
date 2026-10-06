//! Disruptor benchmark scenarios that run anywhere.
//!
//! Each scenario is written once, generic over two platform hooks:
//!
//! * [`Clock`]: a timestamp source. On the host that's `Instant`; on a
//!   target, a cycle counter (`DWT.CYCCNT`, `CNTVCT_EL0`, `mcycle`).
//! * [`Executor`]: how a scenario's [`Task`]s run side by side. On the host
//!   each gets a thread; on a target, use cores or ISRs, or [`Interleaved`].
//!
//! Every scenario checks its results, so each benchmark run is also a
//! correctness test.
#![no_std]

#[cfg(feature = "std")]
extern crate std;

use core::task::Poll;

pub mod baseline;
#[cfg(feature = "std")]
pub mod host;
pub mod scenarios;
mod stats;

pub use stats::{Latency, Throughput};

/// A monotonic timestamp source.
pub trait Clock: Sync {
    fn now(&self) -> u64;
    fn ticks_per_second(&self) -> u64;
}

/// One participant in a scenario, such as a producer or a processor.
///
/// Each `step` does a small, bounded amount of work, so tasks can share a
/// single core. Once a task is `Ready`, stepping it again does nothing.
pub trait Task: Send {
    fn step(&mut self) -> Poll<()>;
}

/// `Ready` once `finished`.
pub(crate) fn done(finished: bool) -> Poll<()> {
    if finished {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

/// Runs tasks side by side until all of them are `Ready`.
pub trait Executor {
    fn run(&self, tasks: &mut [&mut dyn Task]);
}

/// Round-robins every task on the current core.
pub struct Interleaved;

impl Executor for Interleaved {
    fn run(&self, tasks: &mut [&mut dyn Task]) {
        while tasks
            .iter_mut()
            .fold(false, |pending, t| t.step().is_pending() | pending)
        {}
    }
}
