//! The Disruptor's behavioral guarantees, exercised on the host with threads.

use std::thread;

use lmax_disruptor::{Disruptor, InsufficientCapacity, Multi, ProducerType, RingBuffer, Single};

const EVENTS: i64 = 100_000;

/// Records every `(event, sequence)` it sees and how many batches ended.
#[derive(Default)]
struct Recorder {
    seen: Vec<(i64, i64)>,
    batches: usize,
}

impl lmax_disruptor::EventHandler<i64> for Recorder {
    fn on_event(&mut self, event: &i64, sequence: i64, end_of_batch: bool) {
        self.seen.push((*event, sequence));
        self.batches += end_of_batch as usize;
    }
}

/// Events `0..count`, each carrying its own sequence, each seen once.
fn assert_in_order(seen: &[(i64, i64)], count: i64) {
    let expected: Vec<_> = (0..count).map(|s| (s, s)).collect();
    assert_eq!(seen, expected);
}

fn ignore(_: &i64, _: i64, _: bool) {}

#[test]
fn every_event_seen_once_in_order_across_wraparound() {
    let mut ring = RingBuffer::<i64, 64>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut processor = d.handle_events_with(Recorder::default());
    let mut producer = d.start();

    thread::scope(|s| {
        s.spawn(move || (0..EVENTS).for_each(|_| producer.publish_event(|e, seq| *e = seq)));
        while processor.sequence() < EVENTS - 1 {
            processor.poll();
        }
    });

    assert_in_order(&processor.into_handler().seen, EVENTS);
}

#[test]
fn nothing_is_visible_until_published() {
    fn check<P: ProducerType>() {
        let mut ring = RingBuffer::<i64, 16, P>::new(|| -1);
        let mut d = Disruptor::new(&mut ring);
        let mut processor = d.handle_events_with(ignore);
        let mut producer = d.start();

        producer.publish_event(|_, _| assert_eq!(processor.poll(), 0));
        assert_eq!(processor.poll(), 1);
        producer.publish_events(4, |_, _| assert_eq!(processor.poll(), 0));
        assert_eq!(processor.poll(), 4, "a batch is visible at once");
    }
    check::<Single>();
    check::<Multi>();
}

#[test]
fn try_publish_fails_when_full_until_handler_catches_up() {
    let mut ring = RingBuffer::<i64, 4>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut processor = d.handle_events_with(ignore);
    let mut producer = d.start();

    producer.try_publish_events(4, |_, _| {}).unwrap();
    assert_eq!(
        producer.try_publish_event(|_, _| {}),
        Err(InsufficientCapacity)
    );

    processor.poll();
    producer.try_publish_events(4, |_, _| {}).unwrap();
    assert_eq!(
        producer.try_publish_event(|_, _| {}),
        Err(InsufficientCapacity)
    );
}

#[test]
fn end_of_batch_marks_last_available_event() {
    let mut ring = RingBuffer::<i64, 16>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut processor = d.handle_events_with(Recorder::default());
    let mut producer = d.start();

    producer.publish_events(5, |e, seq| *e = seq);
    assert_eq!(processor.poll(), 5);
    producer.publish_event(|e, seq| *e = seq);
    assert_eq!(processor.poll(), 1);

    assert_eq!(processor.into_handler().batches, 2);
}

#[test]
fn multicast_handlers_each_see_every_event() {
    let mut ring = RingBuffer::<i64, 64>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut processors = [(); 3].map(|_| d.handle_events_with(Recorder::default()));
    let mut producer = d.start();

    thread::scope(|s| {
        s.spawn(move || (0..EVENTS).for_each(|_| producer.publish_event(|e, seq| *e = seq)));
        for p in &mut processors {
            s.spawn(move || {
                while p.sequence() < EVENTS - 1 {
                    p.poll();
                }
            });
        }
    });

    for p in processors {
        assert_in_order(&p.into_handler().seen, EVENTS);
    }
}

#[test]
fn downstream_handlers_never_overtake_upstream() {
    let mut ring = RingBuffer::<i64, 64>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut first = d.handle_events_with(Recorder::default());
    let mut second = d
        .after(first.gate())
        .handle_events_with(Recorder::default());
    let mut third = d
        .after(second.gate())
        .handle_events_with(Recorder::default());
    let mut producer = d.start();

    producer.publish_event(|e, seq| *e = seq);
    assert_eq!(second.poll(), 0, "gated on first");
    assert_eq!(first.poll(), 1);
    assert_eq!(third.poll(), 0, "gated on second");
    assert_eq!(second.poll(), 1);
    assert_eq!(third.poll(), 1);

    thread::scope(|s| {
        s.spawn(move || (1..EVENTS).for_each(|_| producer.publish_event(|e, seq| *e = seq)));
        let (a, b, c) = (&mut first, &mut second, &mut third);
        s.spawn(move || {
            while a.sequence() < EVENTS - 1 {
                a.poll();
            }
        });
        s.spawn(move || {
            while b.sequence() < EVENTS - 1 {
                b.poll();
            }
        });
        s.spawn(move || {
            while c.sequence() < EVENTS - 1 {
                c.poll();
            }
        });
    });

    for p in [first, second, third] {
        assert_in_order(&p.into_handler().seen, EVENTS);
    }
}

#[test]
fn diamond_join_waits_for_both_branches() {
    let mut ring = RingBuffer::<i64, 8>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut left = d.handle_events_with(ignore);
    let mut right = d.handle_events_with(ignore);
    let mut join = d
        .after(left.gate() | right.gate())
        .handle_events_with(ignore);
    let mut producer = d.start();

    producer.publish_event(|_, _| {});
    left.poll();
    assert_eq!(join.poll(), 0);
    right.poll();
    assert_eq!(join.poll(), 1);
}

#[test]
fn producer_waits_for_the_last_handler_in_a_pipeline() {
    let mut ring = RingBuffer::<i64, 4>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut first = d.handle_events_with(ignore);
    let mut second = d.after(first.gate()).handle_events_with(ignore);
    let mut producer = d.start();

    producer.try_publish_events(4, |_, _| {}).unwrap();
    first.poll();
    assert_eq!(
        producer.try_publish_event(|_, _| {}),
        Err(InsufficientCapacity)
    );
    second.poll();
    assert_eq!(producer.try_publish_event(|_, _| {}), Ok(()));
}

#[test]
fn multi_producer_sequences_are_gap_free_and_per_producer_ordered() {
    const PRODUCERS: i64 = 4;
    const EACH: i64 = EVENTS / PRODUCERS;
    let mut ring = RingBuffer::<i64, 64, Multi>::new(|| -1);
    let mut d = Disruptor::new(&mut ring);
    let mut processor = d.handle_events_with(Recorder::default());
    let producer = d.start();

    thread::scope(|s| {
        for id in 0..PRODUCERS {
            let mut producer = producer.clone();
            s.spawn(move || {
                (0..EACH).for_each(|n| producer.publish_event(|e, _| *e = id * EACH + n))
            });
        }
        while processor.sequence() < PRODUCERS * EACH - 1 {
            processor.poll();
        }
    });

    let seen = processor.into_handler().seen;
    let mut next = [0; PRODUCERS as usize];
    for (i, (value, sequence)) in seen.into_iter().enumerate() {
        assert_eq!(sequence, i as i64, "gap-free");
        let (id, n) = ((value / EACH) as usize, value % EACH);
        assert_eq!(n, next[id], "per-producer order");
        next[id] += 1;
    }
    assert_eq!(next, [EACH; PRODUCERS as usize]);
}

#[test]
fn a_ring_can_be_rewired_once_its_graph_is_dropped() {
    let mut ring = RingBuffer::<i64, 4>::new(|| -1);
    for _ in 0..2 {
        let mut d = Disruptor::new(&mut ring);
        let mut processor = d.handle_events_with(Recorder::default());
        let mut producer = d.start();
        producer.publish_events(3, |e, seq| *e = seq);
        processor.poll();
        assert_in_order(&processor.into_handler().seen, 3);
    }
}
