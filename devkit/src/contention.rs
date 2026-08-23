// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Generic N-writer paired-trial contention harness: time two named, arbitrary
//! [`ContentionTarget`]s side by side across a swept writer count, in one randomized
//! cross-`(writer count, trial)` schedule, and return the retained samples for the caller's own
//! statistics and reporting.
//!
//! Reporting (tables, trend tests, a model fit) is deliberately not this module's job: it is
//! specific to whatever two arms a caller actually compares. This module owns only the timing and
//! the pairing.

use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::SeedableRng;

/// One arm's shared target: whatever a caller is contending `n` writer threads on, behind a lock
/// of its own choosing.
pub trait ContentionTarget: Send + Sync {
    /// Insert `key` — one call, one logical write, however the implementation locks around it.
    fn insert(&self, key: u64);
}

/// Run `n` writer threads, each inserting `ops` fresh, disjoint keys into `target` (keyed past
/// `prefill` so no writer's insert collides with a pre-fill or with another writer's block),
/// starting together via a barrier so the timed region is genuinely concurrent rather than
/// staggered by thread-spawn latency. Returns the wall-clock time of the concurrent phase alone.
pub fn timed_concurrent_insert(
    target: &(impl ContentionTarget + ?Sized),
    n: usize,
    ops: usize,
    prefill: usize,
) -> Duration {
    let barrier = Barrier::new(n);
    let start = Instant::now();
    thread::scope(|scope| {
        for t in 0..n {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                let base = prefill as u64 + (t * ops) as u64;
                for i in 0..ops as u64 {
                    target.insert(base + i);
                }
            });
        }
    });
    start.elapsed()
}

/// Throughput in ops/s for one trial: `n * ops` inserts over the timed concurrent phase.
pub fn throughput_ops_per_sec(elapsed: Duration, n: usize, ops: usize) -> f64 {
    (n * ops) as f64 / elapsed.as_secs_f64()
}

/// The retained trials for one writer count, both arms named `a`/`b`.
pub struct Point {
    pub n: usize,
    pub a: Vec<f64>,
    pub b: Vec<f64>,
    /// `a / b` **within a trial**. Pairing matters: a machine-wide disturbance during trial `t`
    /// moves both arms, and dividing inside the trial cancels it. A ratio of the two
    /// separately-computed means would keep that noise instead.
    pub ratio: Vec<f64>,
    /// The same ratios, split by which arm ran first in the trial — how a caller tells a real
    /// order effect from ordinary noise, rather than leaving an unexplained spread unexplained.
    pub ratio_a_first: Vec<f64>,
    pub ratio_b_first: Vec<f64>,
    /// `1/X_a − 1/X_b` **within a trial**, in nanoseconds per operation. If both arms sit behind a
    /// lock of the same shape, this cancels the lock's own per-acquisition cost and bounds the two
    /// arms' *own* per-operation cost difference from above.
    pub delta_ns_per_op: Vec<f64>,
}

/// One paired trial: both arms, back to back, in the order `a_first` asks for. Returns
/// `(a's measurement, b's measurement)`.
///
/// Alternating which arm measures first (across trials, by the caller) keeps an order effect out
/// of the mean rather than assuming there is none — see [`Point::ratio_a_first`].
pub fn paired_trial(
    a_first: bool,
    measure_a: &mut dyn FnMut() -> f64,
    measure_b: &mut dyn FnMut() -> f64,
) -> (f64, f64) {
    if a_first {
        let a = measure_a();
        (a, measure_b())
    } else {
        let b = measure_b();
        (measure_a(), b)
    }
}

/// Run `trials` paired trials for every writer count in `counts`, in **one randomized schedule
/// across the whole sweep** rather than a loop per writer count.
///
/// Why not a loop per count: running all of one count's trials consecutively makes the block of
/// wall-clock they occupy part of the treatment — a co-tenant spike, a thermal excursion or a page
/// cache eviction lasting tens of seconds lands entirely on whichever count was running, and shows
/// up as a property of that count. Interleaving every `(count, trial)` in shuffled order spreads
/// any such episode across every count, converting that bias into variance the caller's own
/// statistics can then report honestly.
///
/// `warmup` paired trials at the widest count run first and are discarded — the largest count
/// spawns the most threads and touches the most fresh pages, so it warms what the rest of the
/// sweep will reuse, absorbing process-startup effects (first-touch page faults, frequency ramp)
/// before anything is retained.
///
/// `measure_a`/`measure_b` are called with the writer count for one trial and must return that
/// trial's throughput (or whatever other per-trial scalar the caller is comparing). `on_trial`,
/// when `Some`, is called once per retained trial with `(n, a, b, a_first)` — the hook a caller
/// uses to print a raw per-trial line as the sweep runs, rather than only after it completes.
pub fn run_sweep(
    counts: &[usize],
    trials: usize,
    warmup: usize,
    schedule_seed: u64,
    mut measure_a: impl FnMut(usize) -> f64,
    mut measure_b: impl FnMut(usize) -> f64,
    mut on_trial: Option<&mut dyn FnMut(usize, f64, f64, bool)>,
) -> Vec<Point> {
    let mut points: Vec<Point> = counts
        .iter()
        .map(|&n| Point {
            n,
            a: Vec::with_capacity(trials),
            b: Vec::with_capacity(trials),
            ratio: Vec::with_capacity(trials),
            ratio_a_first: Vec::new(),
            ratio_b_first: Vec::new(),
            delta_ns_per_op: Vec::with_capacity(trials),
        })
        .collect();

    if let Some(&widest) = counts.iter().max() {
        for trial in 0..warmup {
            let a_first = trial % 2 == 0;
            paired_trial(a_first, &mut || measure_a(widest), &mut || {
                measure_b(widest)
            });
        }
    }

    let mut schedule: Vec<(usize, usize)> = (0..points.len())
        .flat_map(|index| (0..trials).map(move |trial| (index, trial)))
        .collect();
    schedule.shuffle(&mut rand::rngs::StdRng::seed_from_u64(schedule_seed));

    for (index, trial) in schedule {
        let n = points[index].n;
        let a_first = trial % 2 == 0;
        let (a, b) = paired_trial(a_first, &mut || measure_a(n), &mut || measure_b(n));
        if let Some(callback) = on_trial.as_deref_mut() {
            callback(n, a, b, a_first);
        }
        let point = &mut points[index];
        point.a.push(a);
        point.b.push(b);
        let ratio = a / b;
        point.ratio.push(ratio);
        if a_first {
            point.ratio_a_first.push(ratio);
        } else {
            point.ratio_b_first.push(ratio);
        }
        const NANOS_PER_SEC: f64 = 1e9;
        point
            .delta_ns_per_op
            .push(NANOS_PER_SEC * (1.0 / a - 1.0 / b));
    }
    points
}

#[cfg(test)]
mod tests;
