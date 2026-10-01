// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license.

//! Compose measured per-session state-repair points into explicit workload distributions.
//!
//! This is deliberately a report rather than a Criterion timing benchmark: the workload model asks for
//! expected/tail costs derived from already-measured per-d curves, not another
//! expensive rerun of the underlying protocols.

#[derive(Clone, Copy, Debug)]
struct Point {
    d: usize,
    bytes: f64,
    interactions: f64,
    prepared_cpu_ms: f64,
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    probability: f64,
    d: usize,
}

#[derive(Clone, Copy)]
struct Workload {
    name: &'static str,
    buckets: &'static [Bucket],
}

const HEALTHY_HEAVY: &[Bucket] = &[
    Bucket {
        probability: 0.90,
        d: 0,
    },
    Bucket {
        probability: 0.08,
        d: 100,
    },
    Bucket {
        probability: 0.019,
        d: 1_000,
    },
    Bucket {
        probability: 0.001,
        d: 10_000,
    },
];
const CONTINUOUS_DRIFT: &[Bucket] = &[
    Bucket {
        probability: 0.40,
        d: 0,
    },
    Bucket {
        probability: 0.45,
        d: 100,
    },
    Bucket {
        probability: 0.14,
        d: 1_000,
    },
    Bucket {
        probability: 0.01,
        d: 10_000,
    },
];
const INTERMITTENT_EDGE: &[Bucket] = &[
    Bucket {
        probability: 0.60,
        d: 0,
    },
    Bucket {
        probability: 0.25,
        d: 100,
    },
    Bucket {
        probability: 0.10,
        d: 1_000,
    },
    Bucket {
        probability: 0.05,
        d: 10_000,
    },
];
const STALE_REPLICA: &[Bucket] = &[
    Bucket {
        probability: 0.70,
        d: 0,
    },
    Bucket {
        probability: 0.10,
        d: 100,
    },
    Bucket {
        probability: 0.10,
        d: 1_000,
    },
    Bucket {
        probability: 0.10,
        d: 10_000,
    },
];

const WORKLOADS: &[Workload] = &[
    Workload {
        name: "healthy-heavy",
        buckets: HEALTHY_HEAVY,
    },
    Workload {
        name: "continuous-small-drift",
        buckets: CONTINUOUS_DRIFT,
    },
    Workload {
        name: "intermittent-edge",
        buckets: INTERMITTENT_EDGE,
    },
    Workload {
        name: "stale-replica",
        buckets: STALE_REPLICA,
    },
];

// Initial measured inputs at n=100k from the repository's state-repair benchmark corpus. Keep the
// source assumptions visible beside every aggregate. Missing interaction/CPU points are intentionally
// not invented; this first slice
// reports bytes and is structured so empirical/per-d CSV inputs can replace these constants later.
const RBSR: &[Point] = &[
    Point {
        d: 0,
        bytes: 39.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 100,
        bytes: 3_600.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 1_000,
        bytes: 19_900.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 10_000,
        bytes: 165_900.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
];
const RIBLT: &[Point] = &[
    Point {
        d: 0,
        bytes: 32.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 100,
        bytes: 3_400.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 1_000,
        bytes: 33_800.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 10_000,
        bytes: 325_400.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
];
const MERKLE: &[Point] = &[
    Point {
        d: 0,
        bytes: 32.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 100,
        bytes: 11_000.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 1_000,
        bytes: 101_700.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
    Point {
        d: 10_000,
        bytes: 942_300.0,
        interactions: 0.0,
        prepared_cpu_ms: 0.0,
    },
];

fn point(points: &[Point], d: usize) -> Point {
    *points
        .iter()
        .find(|p| p.d == d)
        .unwrap_or_else(|| panic!("no measured point for d={d}"))
}

fn expected(workload: Workload, points: &[Point], metric: fn(Point) -> f64) -> f64 {
    workload
        .buckets
        .iter()
        .map(|b| b.probability * metric(point(points, b.d)))
        .sum()
}

fn percentile(workload: Workload, points: &[Point], q: f64, metric: fn(Point) -> f64) -> f64 {
    let mut cumulative = 0.0;
    for bucket in workload.buckets {
        cumulative += bucket.probability;
        if cumulative + f64::EPSILON >= q {
            return metric(point(points, bucket.d));
        }
    }
    panic!("workload probabilities do not cover percentile {q}");
}

fn validate(workload: Workload) {
    let total: f64 = workload.buckets.iter().map(|b| b.probability).sum();
    assert!(
        (total - 1.0).abs() < 1e-9,
        "{} probabilities sum to {total}",
        workload.name
    );
    assert!(workload.buckets.windows(2).all(|w| w[0].d <= w[1].d));
}

fn report(name: &str, workload: Workload, points: &[Point]) {
    let bytes = |p: Point| p.bytes;
    let expected_bytes = expected(workload, points, bytes);
    let large = workload
        .buckets
        .iter()
        .filter(|b| b.d >= 10_000)
        .map(|b| b.probability * point(points, b.d).bytes)
        .sum::<f64>();
    println!(
        "{:<22} {:<7} expected={:>10.1} B  p50={:>9.1}  p90={:>9.1}  p99={:>9.1}  large-share={:>6.2}%",
        workload.name, name, expected_bytes,
        percentile(workload, points, 0.50, bytes),
        percentile(workload, points, 0.90, bytes),
        percentile(workload, points, 0.99, bytes),
        if expected_bytes == 0.0 { 0.0 } else { 100.0 * large / expected_bytes },
    );
    // Keep these fields exercised until measured interaction/CPU curves are populated.
    let _ = expected(workload, points, |p| p.interactions);
    let _ = expected(workload, points, |p| p.prepared_cpu_ms);
}

fn main() {
    println!("Synthetic workload assumptions; not production-representative.");
    println!("Measured byte inputs: repository state-repair corpus, n=100k; outside-range points for d>0.");
    for &workload in WORKLOADS {
        validate(workload);
        report("RBSR", workload, RBSR);
        report("RIBLT", workload, RIBLT);
        report("Merkle", workload, MERKLE);
    }
}
