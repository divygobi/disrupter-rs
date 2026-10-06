//! Disruptor scenarios, modelled on LMAX's performance tests.
//!
//! Events are `i64`s holding their own sequence number. Each handler sums
//! what it sees, and every scenario checks the sums before returning.

use core::task::Poll;

use lmax_disruptor::{
    BatchEventProcessor, Disruptor, EventHandler, Multi, Producer, ProducerType, RingBuffer, Single,
};

use crate::{Clock, Executor, Latency, Task, Throughput, done};

/// Sums every event it sees.
#[derive(Default)]
pub struct Sum(pub i64);

impl EventHandler<i64> for Sum {
    fn on_event(&mut self, event: &i64, _: i64, _: bool) {
        self.0 += event;
    }
}

/// Publishes `remaining` events, `batch` at a time, without blocking.
struct Publish<'a, const N: usize, P: ProducerType> {
    producer: Producer<'a, i64, N, P>,
    remaining: usize,
    batch: usize,
}

impl<const N: usize, P: ProducerType> Task for Publish<'_, N, P> {
    fn step(&mut self) -> Poll<()> {
        let n = self.batch.min(self.remaining);
        if n > 0
            && self
                .producer
                .try_publish_events(n, |e, seq| *e = seq)
                .is_ok()
        {
            self.remaining -= n;
        }
        done(self.remaining == 0)
    }
}

/// Polls until the processor has handled `events` events.
struct Consume<'a, H, const N: usize, P: ProducerType> {
    processor: BatchEventProcessor<'a, i64, H, N, P>,
    events: usize,
}

impl<H: EventHandler<i64> + Send, const N: usize, P: ProducerType> Task for Consume<'_, H, N, P> {
    fn step(&mut self) -> Poll<()> {
        self.processor.poll();
        done(self.processor.sequence() + 1 >= self.events as i64)
    }
}

impl<const N: usize, P: ProducerType> Consume<'_, Sum, N, P> {
    fn assert_saw_all(&self) {
        let n = self.events as i64;
        assert_eq!(
            self.processor.handler().0,
            n * (n - 1) / 2,
            "handler missed events"
        );
    }
}

/// One producer and one handler. With `batch` above 1, the producer
/// publishes `batch` events at a time.
pub fn p1c1<const N: usize>(
    clock: &impl Clock,
    exec: &impl Executor,
    events: usize,
    batch: usize,
) -> Throughput {
    let mut ring = RingBuffer::<i64, N>::new(|| 0);
    let mut d = Disruptor::new(&mut ring);
    let mut consume = Consume {
        processor: d.handle_events_with(Sum::default()),
        events,
    };
    let mut publish = Publish {
        producer: d.start(),
        remaining: events,
        batch,
    };

    let result = Throughput::measure(clock, events, || {
        exec.run(&mut [&mut publish, &mut consume])
    });
    consume.assert_saw_all();
    result
}

/// Three producers on a [`Multi`] ring and one handler. `events` is rounded
/// down to a multiple of 3.
pub fn p3c1<const N: usize>(clock: &impl Clock, exec: &impl Executor, events: usize) -> Throughput {
    let each = events / 3;
    let mut ring = RingBuffer::<i64, N, Multi>::new(|| 0);
    let mut d = Disruptor::new(&mut ring);
    let mut consume = Consume {
        processor: d.handle_events_with(Sum::default()),
        events: each * 3,
    };
    let producer = d.start();
    let mut publishers = [(); 3].map(|_| Publish {
        producer: producer.clone(),
        remaining: each,
        batch: 1,
    });

    let [a, b, c] = &mut publishers;
    let result = Throughput::measure(clock, each * 3, || exec.run(&mut [a, b, c, &mut consume]));
    consume.assert_saw_all();
    result
}

/// One producer feeding three handlers in a pipeline: H1 → H2 → H3.
pub fn p1c3_pipeline<const N: usize>(
    clock: &impl Clock,
    exec: &impl Executor,
    events: usize,
) -> Throughput {
    let mut ring = RingBuffer::<i64, N>::new(|| 0);
    let mut d = Disruptor::new(&mut ring);
    let h1 = d.handle_events_with(Sum::default());
    let h2 = d.after(h1.gate()).handle_events_with(Sum::default());
    let h3 = d.after(h2.gate()).handle_events_with(Sum::default());
    let mut consumers = [h1, h2, h3].map(|processor| Consume { processor, events });
    let mut publish = Publish {
        producer: d.start(),
        remaining: events,
        batch: 1,
    };

    let [a, b, c] = &mut consumers;
    let result = Throughput::measure(clock, events, || exec.run(&mut [&mut publish, a, b, c]));
    consumers.iter().for_each(Consume::assert_saw_all);
    result
}

/// One producer feeding three handlers in parallel.
pub fn p1c3_multicast<const N: usize>(
    clock: &impl Clock,
    exec: &impl Executor,
    events: usize,
) -> Throughput {
    let mut ring = RingBuffer::<i64, N>::new(|| 0);
    let mut d = Disruptor::new(&mut ring);
    let mut consumers = [(); 3].map(|_| Consume {
        processor: d.handle_events_with(Sum::default()),
        events,
    });
    let mut publish = Publish {
        producer: d.start(),
        remaining: events,
        batch: 1,
    };

    let [a, b, c] = &mut consumers;
    let result = Throughput::measure(clock, events, || exec.run(&mut [&mut publish, a, b, c]));
    consumers.iter().for_each(Consume::assert_saw_all);
    result
}

/// Echoes each ping back on the pong ring.
struct Echo<'a, const N: usize>(Producer<'a, i64, N, Single>);

impl<const N: usize> EventHandler<i64> for Echo<'_, N> {
    fn on_event(&mut self, event: &i64, _: i64, _: bool) {
        self.0.publish_event(|e, _| *e = *event);
    }
}

/// Sends one ping at a time and times each round trip.
struct Pinger<'a, C, const N: usize> {
    ping: Producer<'a, i64, N, Single>,
    pong: BatchEventProcessor<'a, i64, Sum, N, Single>,
    clock: &'a C,
    in_flight: Option<u64>,
    latency: Latency,
    samples: u64,
}

impl<C: Clock, const N: usize> Task for Pinger<'_, C, N> {
    fn step(&mut self) -> Poll<()> {
        if self.latency.samples == self.samples {
            return Poll::Ready(());
        }
        match self.in_flight {
            None => {
                self.in_flight = Some(self.clock.now());
                self.ping.publish_event(|e, seq| *e = seq);
            }
            Some(sent) if self.pong.poll() > 0 => {
                self.latency.record(self.clock.now() - sent);
                self.in_flight = None;
            }
            Some(_) => {}
        }
        Poll::Pending
    }
}

/// Round-trip latency: ping ring → echo handler → pong ring and back.
pub fn pingpong<const N: usize>(clock: &impl Clock, exec: &impl Executor, samples: u64) -> Latency {
    let mut ping_ring = RingBuffer::<i64, N>::new(|| 0);
    let mut pong_ring = RingBuffer::<i64, N>::new(|| 0);
    let mut pong = Disruptor::new(&mut pong_ring);
    let pong_processor = pong.handle_events_with(Sum::default());
    let mut ping = Disruptor::new(&mut ping_ring);
    let mut echo = Consume {
        processor: ping.handle_events_with(Echo(pong.start())),
        events: samples as usize,
    };
    let mut pinger = Pinger {
        ping: ping.start(),
        pong: pong_processor,
        clock,
        in_flight: None,
        latency: Latency::default(),
        samples,
    };

    exec.run(&mut [&mut pinger, &mut echo]);
    pinger.latency
}
