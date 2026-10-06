//! Other queues run through the same harness, for comparison.

use core::task::Poll;

use heapless::spsc::{Consumer, Producer, Queue};

use crate::{Clock, Executor, Task, Throughput, done};

struct Enqueue<'q, const N: usize> {
    tx: Producer<'q, i64, N>,
    next: i64,
    end: i64,
}

impl<const N: usize> Task for Enqueue<'_, N> {
    fn step(&mut self) -> Poll<()> {
        if self.next < self.end && self.tx.enqueue(self.next).is_ok() {
            self.next += 1;
        }
        done(self.next == self.end)
    }
}

struct Dequeue<'q, const N: usize> {
    rx: Consumer<'q, i64, N>,
    received: i64,
    end: i64,
    sum: i64,
}

impl<const N: usize> Task for Dequeue<'_, N> {
    fn step(&mut self) -> Poll<()> {
        while let Some(value) = self.rx.dequeue() {
            self.sum += value;
            self.received += 1;
        }
        done(self.received == self.end)
    }
}

/// [`p1c1`](crate::scenarios::p1c1) over `heapless::spsc::Queue`.
pub fn heapless_spsc_p1c1<const N: usize>(
    clock: &impl Clock,
    exec: &impl Executor,
    events: usize,
) -> Throughput {
    let mut queue = Queue::<i64, N>::new();
    let (tx, rx) = queue.split();
    let mut enqueue = Enqueue {
        tx,
        next: 0,
        end: events as i64,
    };
    let mut dequeue = Dequeue {
        rx,
        received: 0,
        end: events as i64,
        sum: 0,
    };
    let result = Throughput::measure(clock, events, || {
        exec.run(&mut [&mut enqueue, &mut dequeue])
    });
    let n = events as i64;
    assert_eq!(dequeue.sum, n * (n - 1) / 2);
    result
}
