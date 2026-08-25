// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `K`-writer contention: write throughput vs writer count `N`, for `FingerprintTreeMap` and for
//! plain `BTreeMap`, each behind one shared `parking_lot::RwLock` of the exact shape
//! `src/replica.rs` uses for its `map` field (`Arc<RwLock<FingerprintTreeMap<K, V>>>`).
//!
//! Isolates the RSOS contract's own write cost (#445, #359): the `FingerprintTreeMap` arm pays the
//! lock plus the root-path aggregate maintenance `rsos::fingerprint_tree_map`'s `O(log n)`
//! `Aggregate(l, u)` bound requires; the `BTreeMap` arm pays the same lock and insert shape with no
//! aggregate to maintain. The delta between the two arms, at each `N`, is the contract's own share
//! of the write cost. Full method and results: `benches/README.md`.
//!
//! Reports three quantities (#455), none of them a plain fp/btree ratio — that quotient's two
//! terms both grow with `N`, so it cannot say which one moved:
//!
//! - A machine-independent **counted** result (`rsos::counters`, behind
//!   `--cfg reconcile_internal_testing`): cached aggregates an insert maintains, unaffected by the
//!   host.
//! - A **timed** result over [`TRIALS`] repeated trials per `(N, arm)`, arms paired within a trial
//!   and order-alternated, the whole `(N, trial)` sweep run in one shuffled schedule, reported as
//!   percentile-bootstrap-interval means (`devkit::stats`).
//! - **Delta**, `1/X_fp − 1/X_btree` per trial: cancels the shared lock term
//!   (`1/X_arm = S_arm + H(N)`) to bound the contract's own per-insert cost from above, exact at
//!   `N = 1` — the statistic the report leads with (#457).
//!
//! Throughput stays wall-clock on purpose: lock waiting *is* elapsed time, with no counted proxy
//! for it.
//!
//! **What is, and is not, measured.** Both arms insert into a map pre-filled to [`PREFILL`]
//! entries, then `N` threads each insert their own disjoint block of fresh keys, one `write()`
//! acquisition per key — `Replica::just_insert`/gossip receipt's own shape. This is a **lock
//! contention** benchmark, not a lock-free redesign or a COW prototype — both are #271/#273/#274.
//!
//! **Comparability caveat (#281).** Every timed comparison is arm-against-arm on the machine that
//! produced it; absolute ops/s are not portable across machines. The counted half carries no such
//! caveat.
//!
//! Every parameter is overridable from the environment (#456), and `CONTENTION_RAW=1` emits one
//! line per trial so several invocations can be pooled into the invocation-level statistics
//! `benches/README.md` documents — the experimental unit is the invocation, not the trial:
//!
//! ```sh
//! CONTENTION_WRITERS=1,2,4,8,16,32,64,128 CONTENTION_TRIALS=30 cargo bench --bench contention
//! ```
//!
//! Reproduction and results: `benches/README.md`. Not run in CI (only compile-checked); run locally
//! with `cargo bench --bench contention`.

use std::hint::black_box;
use std::str::FromStr;
use std::time::Duration;

use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use parking_lot::RwLock;

use devkit::contention::{
    run_sweep, throughput_ops_per_sec, timed_concurrent_insert, ContentionTarget, Point,
};
use devkit::stats::{diff_ci, excludes_zero, summarize, Summary};
use reconcile::FingerprintTreeMap;

/// Writer-thread counts swept by default. `1, 2, 4` are below this machine's core count, `8, 16`
/// push past it deliberately — contention past the core count is exactly the regime a lock-free
/// redesign (#271) would target, so the sweep needs to show where it starts, not stop at the core
/// count. Override with `CONTENTION_WRITERS` (#456).
const WRITER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Entries each writer inserts per trial. Large enough that thread-spawn/join overhead (a few µs
/// per thread) is a small fraction of the timed region even at the smallest `N`.
const OPS_PER_WRITER: usize = 20_000;

/// Entries the map is pre-filled to before writers start, outside the timed region. Gives the
/// root path a non-trivial depth (`log₆ 100 000 ≈ 6`) — an empty map has no aggregate maintenance
/// worth contending on, which would understate the RSOS arm's cost.
const PREFILL: usize = 100_000;

/// Trials per `(N, arm)` retained for the statistics. #455 asks for 20–30; 30 is the top of that
/// band and still costs seconds, since one trial is already `N * OPS_PER_WRITER` inserts.
const TRIALS: usize = 30;

/// Paired trials run and discarded before the sweep proper, at its largest `N` (the most thread
/// creation and allocation of any point). Absorbs process-startup effects — first-touch page
/// faults, CPU frequency ramp — which would otherwise be charged to whichever trial the schedule
/// happens to draw first.
const WARMUP: usize = 3;

/// Seed for the trial schedule's shuffle. Fixed, so a run is reproducible as an experiment: the
/// *measurements* vary, the *design* does not.
const SCHEDULE_SEED: u64 = 20_260_820;

/// Read `name` from the environment, falling back to `default`.
///
/// # Panics
///
/// If `name` is set but unparseable — a typo in a sweep parameter must not be silently ignored,
/// leaving a run that quietly measured the default.
fn env_or<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Err(_) => default,
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|_| panic!("{name}={raw:?} could not be parsed")),
    }
}

/// Cores this machine will actually run threads on, for flagging the writer counts that exceed it.
/// Falls back to 1 — the conservative reading, flagging every contended row rather than none.
fn available_parallelism() -> usize {
    std::thread::available_parallelism().map_or(1, |cores| cores.get())
}

/// The `N` sweep: `CONTENTION_WRITERS` as a comma-separated list, else [`WRITER_COUNTS`].
///
/// # Panics
///
/// If the variable is set but malformed.
fn writer_counts() -> Vec<usize> {
    match std::env::var("CONTENTION_WRITERS") {
        Err(_) => WRITER_COUNTS.to_vec(),
        Ok(raw) => {
            raw.split(',')
                .map(|field| {
                    field.trim().parse().unwrap_or_else(|_| {
                        panic!("CONTENTION_WRITERS field {field:?} is not a count")
                    })
                })
                .collect()
        }
    }
}

struct FingerprintArm(RwLock<FingerprintTreeMap<u64, u64>>);

impl ContentionTarget for FingerprintArm {
    fn insert(&self, key: u64) {
        self.0.write().insert(key, key);
    }
}

struct BTreeArm(RwLock<std::collections::BTreeMap<u64, u64>>);

impl ContentionTarget for BTreeArm {
    fn insert(&self, key: u64) {
        self.0.write().insert(key, key);
    }
}

fn prefilled_fingerprint_arm(prefill: usize) -> FingerprintArm {
    let mut map = FingerprintTreeMap::<u64, u64>::new();
    for key in 0..prefill as u64 {
        map.insert(key, key);
    }
    FingerprintArm(RwLock::new(map))
}

fn prefilled_btree_arm(prefill: usize) -> BTreeArm {
    let mut map = std::collections::BTreeMap::new();
    for key in 0..prefill as u64 {
        map.insert(key, key);
    }
    BTreeArm(RwLock::new(map))
}

/// Run `trials` paired trials for every writer count, `a` = the `FingerprintTreeMap` arm, `b` =
/// the `BTreeMap` control — see [`devkit::contention::run_sweep`] for the schedule/warmup design.
fn run_contention_sweep(counts: &[usize], trials: usize, ops: usize, prefill: usize) -> Vec<Point> {
    let measure_a = |n: usize| {
        let arm = prefilled_fingerprint_arm(prefill);
        let elapsed = timed_concurrent_insert(black_box(&arm), n, ops, prefill);
        throughput_ops_per_sec(elapsed, n, ops)
    };
    let measure_b = |n: usize| {
        let arm = prefilled_btree_arm(prefill);
        let elapsed = timed_concurrent_insert(black_box(&arm), n, ops, prefill);
        throughput_ops_per_sec(elapsed, n, ops)
    };

    let raw = std::env::var("CONTENTION_RAW").is_ok();
    if raw {
        println!(
            "[contention-raw] writers,fingerprint_ops_per_sec,btree_ops_per_sec,fingerprint_first"
        );
    }
    let mut print_raw = |n: usize, a: f64, b: f64, a_first: bool| {
        println!("[contention-raw] {n},{a:.1},{b:.1},{a_first}");
    };
    let on_trial: Option<&mut dyn FnMut(usize, f64, f64, bool)> =
        if raw { Some(&mut print_raw) } else { None };

    run_sweep(
        counts,
        trials,
        WARMUP,
        SCHEDULE_SEED,
        measure_a,
        measure_b,
        on_trial,
    )
}

/// The per-`N` table: both arms and the paired ratio, each with its bootstrap interval.
fn print_throughput_table(points: &[Point], trials: usize, ops: usize, prefill: usize) {
    let cores = available_parallelism();
    println!(
        "[contention] {trials} trials per (N, arm), {ops} inserts/writer, map pre-filled to \
         {prefill} entries; {cores} cores available."
    );
    println!(
        "[contention] Rows marked `!` run more writers than there are cores. Past that point a \
         thread can be preempted while *holding* the lock, stalling every other writer -- and the \
         longer critical section is preempted mid-section more often, so delta inflates for a \
         reason that is the scheduler's, not the contract's. Read those rows as an upper bound \
         only (#456)."
    );
    println!(
        "[contention] Mean with a 95% percentile-bootstrap interval. `delta` is \
         1/X_fp - 1/X_btree: the contract's own per-insert cost, with the shared lock term \
         cancelled (#457)."
    );
    println!(
        "[contention] {:>7} | {:>29} | {:>29} | {:>25} | {:>6} | {:>22}",
        "writers",
        "fingerprint ops/s",
        "btree ops/s",
        "ratio (paired, fp/btree)",
        "median",
        "delta ns/insert"
    );
    for point in points {
        let fingerprint = summarize(&point.a);
        let btree = summarize(&point.b);
        let ratio = summarize(&point.ratio);
        let delta = summarize(&point.delta_ns_per_op);
        println!(
            "[contention] {n:>6}{oversubscribed} | {fp_mean:>9.0} [{fp_lo:.0}, {fp_hi:.0}] | \
             {bt_mean:>9.0} [{bt_lo:.0}, {bt_hi:.0}] | {r_mean:>6.3} [{r_lo:.3}, {r_hi:.3}] | \
             {r_median:>6.3} | {d_mean:>6.0} [{d_lo:.0}, {d_hi:.0}]",
            n = point.n,
            oversubscribed = if point.n > cores { "!" } else { " " },
            fp_mean = fingerprint.mean,
            fp_lo = fingerprint.lo,
            fp_hi = fingerprint.hi,
            bt_mean = btree.mean,
            bt_lo = btree.lo,
            bt_hi = btree.hi,
            r_mean = ratio.mean,
            r_lo = ratio.lo,
            r_hi = ratio.hi,
            r_median = ratio.median,
            d_mean = delta.mean,
            d_lo = delta.lo,
            d_hi = delta.hi,
        );
    }
}

/// The claim #359 made and #455 asks to test properly: does the fp/btree ratio *widen* as writer
/// count grows — that is, does the contract's share of the cost grow with contention?
///
/// Two comparisons, both as bootstrap intervals on a difference of means rather than as an
/// eyeballed overlap of two intervals (overlap is not a test; an interval on the difference is).
fn print_ratio_trend(points: &[Point]) {
    let Some(baseline) = points.first() else {
        return;
    };
    println!(
        "[contention] Does the ratio move with N? Bootstrap interval on the difference of paired \
         ratios; an interval excluding 0 is a real move at 95%."
    );

    println!(
        "[contention] vs N={} (the uncontended point — the contract's cost alone):",
        baseline.n
    );
    for point in points.iter().skip(1) {
        let difference = diff_ci(&point.ratio, &baseline.ratio);
        let verdict = if !excludes_zero(&difference) {
            "indistinguishable"
        } else if difference.mean > 0.0 {
            "DILUTED by contention"
        } else {
            "WIDENED by contention"
        };
        println!(
            "[contention] {:>34} {:>+8.3} [{:+.3}, {:+.3}]  {}",
            format!("ratio(N={}) - ratio(N={})", point.n, baseline.n),
            difference.mean,
            difference.lo,
            difference.hi,
            verdict,
        );
    }

    println!(
        "[contention] And the sharper question -- does the gap grow with the lock term cancelled \
         (delta = 1/X_fp - 1/X_btree, an upper bound on the contract's own cost)?"
    );
    for point in points.iter().skip(1) {
        let difference = diff_ci(&point.delta_ns_per_op, &baseline.delta_ns_per_op);
        println!(
            "[contention] {:>34} {:>+8.0} [{:+.0}, {:+.0}] ns  {}",
            format!("delta(N={}) - delta(N={})", point.n, baseline.n),
            difference.mean,
            difference.lo,
            difference.hi,
            if !excludes_zero(&difference) {
                "indistinguishable"
            } else if difference.mean > 0.0 {
                "the gap GROWS with contention"
            } else {
                "the gap SHRINKS with contention"
            },
        );
    }

    // Among contended points only: N=1 has no lock waiting at all, so including it would confound
    // "the ratio changes once a lock is contended" with "the ratio changes as contention deepens".
    let contended: Vec<&Point> = points.iter().filter(|point| point.n > 1).collect();
    if let (Some(first), Some(last)) = (contended.first(), contended.last()) {
        if first.n != last.n {
            let difference = diff_ci(&last.ratio, &first.ratio);
            println!(
                "[contention] Across contended N only, ratio(N={}) - ratio(N={}): \
                 {:+.3} [{:+.3}, {:+.3}] -- {}",
                last.n,
                first.n,
                difference.mean,
                difference.lo,
                difference.hi,
                if excludes_zero(&difference) {
                    "the ratio does move as contention deepens"
                } else {
                    "no detectable trend as contention deepens"
                },
            );
        }
    }

    // Interval geometry, which #455 asks to see alongside the tests above. Reported as a fact about
    // the intervals, never as a substitute for the difference tests: overlapping intervals do not
    // imply the means agree.
    let ratios: Vec<(usize, Summary)> = points
        .iter()
        .map(|point| (point.n, summarize(&point.ratio)))
        .collect();
    let disjoint: Vec<String> = ratios
        .iter()
        .enumerate()
        .flat_map(|(i, (n, a))| {
            ratios[i + 1..]
                .iter()
                .filter(move |(_, b)| a.disjoint_from(b))
                .map(move |(m, _)| format!("{n}/{m}"))
        })
        .collect();
    println!(
        "[contention] Ratio intervals that do not overlap: {}",
        if disjoint.is_empty() {
            "none".to_string()
        } else {
            disjoint.join(", ")
        }
    );
}

/// #457's model, checked against the data it is meant to explain.
///
/// **Assumptions.** `N` writers in a closed loop, each acquiring one exclusive lock, doing the whole
/// operation inside it, releasing and immediately retrying — no think time. The lock is a single
/// server, so system-wide seconds per operation is the critical section plus whatever an
/// acquisition costs at that writer count: `1/X_arm(N) = S_arm(N) + H(N)`. `H` is a property of the
/// lock and the contention level, not of what runs inside — the two arms use the same lock type
/// and the same acquisition pattern — so it is common to both.
///
/// **The null this tests.** Take `S_fp − S_btree` to be constant in `N`: the contract does a fixed
/// amount of extra work per insert, and contention only adds lock time on top. Measure that
/// constant where nothing waits and the cancellation is exact (`Δ` at the lowest `N`), then
/// *predict* the RSOS arm from the control arm at every other `N`:
///
/// ```text
/// X_fp_predicted(N) = 1 / ( 1/X_btree(N) + Δ(N_min) )
/// ```
///
/// One parameter, fitted at one point, extrapolated everywhere else — so a residual is a statement
/// about the model, not a fit artefact. Nothing here names `FingerprintTreeMap`: it applies to any
/// structure whose per-operation critical section is longer than a baseline's by a fixed amount,
/// under any global lock.
fn print_model_fit(points: &[Point]) {
    let Some(baseline) = points.first() else {
        return;
    };
    let fixed_cost_secs = summarize(&baseline.delta_ns_per_op).mean * 1e-9;
    println!(
        "[contention] Model (#457): predict the RSOS arm from the control arm assuming the \
         contract's own cost is the constant {:.0} ns measured at N={}.",
        fixed_cost_secs * 1e9,
        baseline.n
    );
    for point in points.iter().skip(1) {
        let btree = summarize(&point.b).mean;
        let measured = summarize(&point.a).mean;
        let predicted = 1.0 / (1.0 / btree + fixed_cost_secs);
        println!(
            "[contention] {:>34} predicted {predicted:>9.0}  measured {measured:>9.0}  \
             residual {:>+6.1}%",
            format!("N={}", point.n),
            100.0 * (measured - predicted) / predicted,
        );
    }
    println!(
        "[contention] A residual growing more negative with N falsifies the null: the gap between \
         the arms is not a constant, it rises with writer count."
    );
}

/// Validate the harness, not the subject: does running first or second systematically change an
/// arm's throughput?
///
/// Alternating the order keeps such an effect out of the mean, so this cannot invalidate the table
/// above — but an effect large enough to detect belongs in the write-up, because it is most of the
/// dispersion the `cv` column reports and a reader would otherwise read that dispersion as
/// measurement noise.
fn print_order_effect(points: &[Point]) {
    println!(
        "[contention] Order effect (harness check): mean paired ratio when the fingerprint arm ran \
         first, minus when it ran second."
    );
    for point in points {
        if point.ratio_a_first.is_empty() || point.ratio_b_first.is_empty() {
            continue;
        }
        let difference = diff_ci(&point.ratio_a_first, &point.ratio_b_first);
        println!(
            "[contention] {:>34} {:>+8.3} [{:+.3}, {:+.3}]  {}",
            format!("N={}", point.n),
            difference.mean,
            difference.lo,
            difference.hi,
            if excludes_zero(&difference) {
                "order matters -- alternation is load-bearing"
            } else {
                "no detectable order effect"
            },
        );
    }
}

/// The machine-independent half (#454's "stands on its own outside this repo").
///
/// Reports how many cached aggregates one insert maintains — the work the RSOS contract mandates
/// and a plain `BTreeMap`, doing the same descent with no summary to keep, does not do at all. Run
/// single-threaded, untimed, outside any lock: the number is deterministic, so one pass
/// characterizes every writer count, and nothing here can perturb the timed phases above.
#[cfg(reconcile_internal_testing)]
fn print_counted_summary(prefill: usize) {
    use rsos::counters;

    let mut map = FingerprintTreeMap::<u64, u64>::new();
    for key in 0..prefill as u64 {
        map.insert(key, key);
    }

    const PROBE: u64 = 4_096;
    let before = counters::snapshot();
    for i in 0..PROBE {
        map.insert(prefill as u64 + i, i);
    }
    let fresh = (counters::snapshot() - before).aggregate_updates;

    let before = counters::snapshot();
    for i in 0..PROBE {
        map.insert(i, i + 1);
    }
    let overwrite = (counters::snapshot() - before).aggregate_updates;

    println!(
        "[contention] Counted (machine-independent), map of {prefill} entries, {PROBE} probes:"
    );
    println!(
        "[contention] {:>34} {:.2}   (BTreeMap control: 0.00, by construction)",
        "aggregate updates / fresh insert",
        fresh as f64 / PROBE as f64,
    );
    println!(
        "[contention] {:>34} {:.2}   -- one per level of the key's root path",
        "aggregate updates / overwrite",
        overwrite as f64 / PROBE as f64,
    );
}

#[cfg(not(reconcile_internal_testing))]
fn print_counted_summary(_prefill: usize) {
    println!(
        "[contention] Counted half skipped: rebuild with \
         `RUSTFLAGS='--cfg reconcile_internal_testing'` for the machine-independent \
         aggregate-update counts."
    );
}

/// Printed report: the counted result, then throughput vs `N` for both arms with intervals, then
/// the explicit test of whether the ratio moves with `N`. Meant to be read directly and copied into
/// `benches/README.md`; the Criterion groups below plot the same measurement.
fn print_contention_report() {
    let trials = env_or("CONTENTION_TRIALS", TRIALS);
    let ops = env_or("CONTENTION_OPS", OPS_PER_WRITER);
    let prefill = env_or("CONTENTION_PREFILL", PREFILL);

    print_counted_summary(prefill);

    let points = run_contention_sweep(&writer_counts(), trials, ops, prefill);

    print_throughput_table(&points, trials, ops, prefill);
    print_ratio_trend(&points);
    print_model_fit(&points);
    print_order_effect(&points);
}

/// Timed Criterion groups for both arms, over the same `N` sweep, with Criterion's own sampling and
/// `Throughput::Elements` so `target/criterion/report/index.html` plots the same measurement the
/// report above states. The report, not this group, is what `benches/README.md` quotes: it pairs the
/// arms within a trial and puts an interval on their ratio, which Criterion — measuring each
/// benchmark id independently — cannot do.
fn writer_contention(c: &mut Criterion) {
    print_contention_report();

    let ops = env_or("CONTENTION_OPS", OPS_PER_WRITER);
    let prefill = env_or("CONTENTION_PREFILL", PREFILL);

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("writer_contention");
    group.plot_config(plot_config);

    for n in writer_counts() {
        group.throughput(Throughput::Elements((n * ops) as u64));
        // Fewer samples at high N: each sample is already N * ops inserts, and Criterion needs a
        // fresh, freshly pre-filled map per sample (the timed region must start from the same tree
        // depth every time), which itself costs O(prefill).
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("fingerprint_tree_map", n),
            &n,
            |bencher, &n| {
                bencher.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let arm = prefilled_fingerprint_arm(prefill);
                        total += timed_concurrent_insert(black_box(&arm), n, ops, prefill);
                    }
                    total
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("btree_map", n), &n, |bencher, &n| {
            bencher.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let arm = prefilled_btree_arm(prefill);
                    total += timed_concurrent_insert(black_box(&arm), n, ops, prefill);
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, writer_contention);
criterion_main!(benches);
