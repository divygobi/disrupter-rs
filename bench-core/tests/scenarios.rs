//! Every scenario completes and passes its own checks on both executors.

use bench_core::host::{HostClock, Threaded, crossbeam_p1c1};
use bench_core::{Executor, Interleaved, baseline, scenarios};

const EVENTS: usize = 20_000;

fn run_all(exec: &impl Executor) {
    let clock = HostClock::default();
    for batch in [1, 7, 64] {
        scenarios::p1c1::<64>(&clock, exec, EVENTS, batch);
    }
    scenarios::p3c1::<64>(&clock, exec, EVENTS);
    scenarios::p1c3_pipeline::<64>(&clock, exec, EVENTS);
    scenarios::p1c3_multicast::<64>(&clock, exec, EVENTS);
    assert_eq!(scenarios::pingpong::<8>(&clock, exec, 1_000).samples, 1_000);
    baseline::heapless_spsc_p1c1::<64>(&clock, exec, EVENTS);
}

#[test]
fn interleaved() {
    run_all(&Interleaved);
}

#[test]
fn threaded() {
    run_all(&Threaded);
    crossbeam_p1c1(&HostClock::default(), 64, EVENTS);
}
