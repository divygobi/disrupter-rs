//! Host implementations of the platform hooks, plus a `crossbeam` baseline.

use std::thread;
use std::time::Instant;

use crossbeam_queue::ArrayQueue;

use crate::{Clock, Executor, Task, Throughput};

/// Nanoseconds since it was created.
pub struct HostClock(Instant);

impl Default for HostClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Clock for HostClock {
    fn now(&self) -> u64 {
        self.0.elapsed().as_nanos() as u64
    }

    fn ticks_per_second(&self) -> u64 {
        1_000_000_000
    }
}

/// Gives each task its own thread.
pub struct Threaded;

impl Executor for Threaded {
    fn run(&self, tasks: &mut [&mut dyn Task]) {
        thread::scope(|s| {
            for task in tasks {
                s.spawn(move || while task.step().is_pending() {});
            }
        });
    }
}

/// [`p1c1`](crate::scenarios::p1c1) over `crossbeam_queue::ArrayQueue`.
pub fn crossbeam_p1c1(clock: &impl Clock, capacity: usize, events: usize) -> Throughput {
    let queue = ArrayQueue::new(capacity);
    let n = events as i64;
    let mut sum = 0;
    let result = Throughput::measure(clock, events, || {
        sum = thread::scope(|s| {
            s.spawn(|| {
                for value in 0..n {
                    while queue.push(value).is_err() {}
                }
            });
            (0..n)
                .map(|_| {
                    loop {
                        if let Some(value) = queue.pop() {
                            break value;
                        }
                    }
                })
                .sum::<i64>()
        })
    });
    assert_eq!(sum, n * (n - 1) / 2);
    result
}
