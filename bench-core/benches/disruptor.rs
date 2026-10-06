//! Host benchmarks: every scenario on real threads, plus baselines.
//!
//! `cargo bench -p bench-core --features std --bench disruptor`

use std::time::Duration;

use bench_core::host::{HostClock, Threaded, crossbeam_p1c1};
use bench_core::{baseline, scenarios};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const BUFFER: usize = 1024;
const EVENTS: usize = 100_000;

fn throughput(c: &mut Criterion) {
    let clock = &HostClock::default();
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(EVENTS as u64));

    let mut bench = |name: &str, run: &dyn Fn() -> bench_core::Throughput| {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| Duration::from_nanos(run().nanos()))
                    .sum()
            })
        });
    };
    bench("p1c1", &|| {
        scenarios::p1c1::<BUFFER>(clock, &Threaded, EVENTS, 1)
    });
    bench("p1c1_batch10", &|| {
        scenarios::p1c1::<BUFFER>(clock, &Threaded, EVENTS, 10)
    });
    bench("p3c1", &|| {
        scenarios::p3c1::<BUFFER>(clock, &Threaded, EVENTS)
    });
    bench("p1c3_pipeline", &|| {
        scenarios::p1c3_pipeline::<BUFFER>(clock, &Threaded, EVENTS)
    });
    bench("p1c3_multicast", &|| {
        scenarios::p1c3_multicast::<BUFFER>(clock, &Threaded, EVENTS)
    });
    bench("heapless_spsc_p1c1", &|| {
        baseline::heapless_spsc_p1c1::<BUFFER>(clock, &Threaded, EVENTS)
    });
    bench("crossbeam_p1c1", &|| crossbeam_p1c1(clock, BUFFER, EVENTS));
    group.finish();
}

fn latency(c: &mut Criterion) {
    let clock = &HostClock::default();
    c.bench_function("latency/pingpong", |b| {
        b.iter_custom(|iters| {
            Duration::from_nanos(scenarios::pingpong::<BUFFER>(clock, &Threaded, iters).total)
        })
    });

    let l = scenarios::pingpong::<BUFFER>(clock, &Threaded, 100_000);
    let (mean, p99) = (l.mean(), l.percentile(99));
    println!(
        "pingpong round trip (ns): min {} mean {mean} p99 <= {p99} max {}",
        l.min, l.max
    );
}

criterion_group!(benches, throughput, latency);
criterion_main!(benches);
