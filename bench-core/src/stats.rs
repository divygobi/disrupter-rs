use crate::Clock;

/// `events` handled in `ticks` of a [`Clock`].
#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    pub events: u64,
    pub ticks: u64,
    pub ticks_per_second: u64,
}

impl Throughput {
    /// Times `run`, which handles `events` events.
    pub fn measure(clock: &impl Clock, events: usize, run: impl FnOnce()) -> Self {
        let start = clock.now();
        run();
        Self {
            events: events as u64,
            ticks: clock.now() - start,
            ticks_per_second: clock.ticks_per_second(),
        }
    }

    /// Ticks per event. With a cycle counter as the clock, this is cycles
    /// per event.
    pub fn ticks_per_event(&self) -> f64 {
        self.ticks as f64 / self.events as f64
    }

    pub fn nanos(&self) -> u64 {
        (self.ticks as u128 * 1_000_000_000 / self.ticks_per_second as u128) as u64
    }
}

/// Latency samples in ticks. A power-of-two histogram keeps it
/// allocation-free: bucket `b` holds values that are `b` bits long.
#[derive(Debug, Clone)]
pub struct Latency {
    pub samples: u64,
    pub min: u64,
    pub max: u64,
    pub total: u64,
    buckets: [u64; 65],
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            samples: 0,
            min: u64::MAX,
            max: 0,
            total: 0,
            buckets: [0; 65],
        }
    }
}

impl Latency {
    pub fn record(&mut self, ticks: u64) {
        self.samples += 1;
        self.min = self.min.min(ticks);
        self.max = self.max.max(ticks);
        self.total += ticks;
        self.buckets[(u64::BITS - ticks.leading_zeros()) as usize] += 1;
    }

    pub fn mean(&self) -> u64 {
        self.total / self.samples.max(1)
    }

    /// The upper bound of the bucket holding the `pct`th percentile.
    pub fn percentile(&self, pct: u64) -> u64 {
        let target = (self.samples * pct).div_ceil(100);
        let mut seen = 0;
        for (bits, count) in (0u32..).zip(self.buckets) {
            seen += count;
            if seen >= target {
                return u64::MAX.checked_shr(u64::BITS - bits).unwrap_or(0);
            }
        }
        self.max
    }
}
