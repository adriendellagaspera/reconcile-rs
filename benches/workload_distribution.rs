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
    interactions: Option<f64>,
    prepared_cpu_ms: Option<f64>,
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
// source assumptions visible beside every aggregate. Missing measurements remain None rather than
// being invented. CPU points for d>0 are the measured outside-range insert curve; interaction counts
// are not transferred from a different spatial profile.
const RBSR: &[Point] = &[
    Point {
        d: 0,
        bytes: 39.0,
        interactions: None,
        prepared_cpu_ms: None,
    },
    Point {
        d: 100,
        bytes: 3_600.0,
        interactions: None,
        prepared_cpu_ms: Some(0.13),
    },
    Point {
        d: 1_000,
        bytes: 19_900.0,
        interactions: None,
        prepared_cpu_ms: Some(0.22),
    },
    Point {
        d: 10_000,
        bytes: 165_900.0,
        interactions: None,
        prepared_cpu_ms: Some(0.97),
    },
];
const RIBLT: &[Point] = &[
    Point {
        d: 0,
        bytes: 32.0,
        interactions: None,
        prepared_cpu_ms: None,
    },
    Point {
        d: 100,
        bytes: 3_400.0,
        interactions: None,
        prepared_cpu_ms: Some(83.0),
    },
    Point {
        d: 1_000,
        bytes: 33_800.0,
        interactions: None,
        prepared_cpu_ms: Some(487.0),
    },
    Point {
        d: 10_000,
        bytes: 325_400.0,
        interactions: None,
        prepared_cpu_ms: Some(4_390.0),
    },
];
const MERKLE: &[Point] = &[
    Point {
        d: 0,
        bytes: 32.0,
        interactions: None,
        prepared_cpu_ms: None,
    },
    Point {
        d: 100,
        bytes: 11_000.0,
        interactions: None,
        prepared_cpu_ms: Some(0.04),
    },
    Point {
        d: 1_000,
        bytes: 101_700.0,
        interactions: None,
        prepared_cpu_ms: Some(0.21),
    },
    Point {
        d: 10_000,
        bytes: 942_300.0,
        interactions: None,
        prepared_cpu_ms: Some(1.92),
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

fn expected_optional(
    workload: Workload,
    points: &[Point],
    metric: fn(Point) -> Option<f64>,
) -> Option<f64> {
    workload.buckets.iter().try_fold(0.0, |sum, bucket| {
        metric(point(points, bucket.d)).map(|value| sum + bucket.probability * value)
    })
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
    match expected_optional(workload, points, |p| p.prepared_cpu_ms) {
        Some(cpu) => println!("  expected prepared CPU: {cpu:.3} ms/session"),
        None => println!("  expected prepared CPU: n/a (curve incomplete)"),
    }
    match expected_optional(workload, points, |p| p.interactions) {
        Some(interactions) => println!("  expected interactions: {interactions:.2}/session"),
        None => println!("  expected interactions: n/a (not measured for this curve)"),
    }
}

fn empirical_workload(path: &str) -> Workload {
    let input = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read empirical workload {path}: {error}"));
    let mut counts = std::collections::BTreeMap::<usize, u64>::new();
    for (line_no, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line == "d,count" {
            continue;
        }
        let (d, count) = line
            .split_once(',')
            .unwrap_or_else(|| panic!("{}:{}: expected d,count", path, line_no + 1));
        *counts.entry(d.trim().parse().expect("invalid d")).or_default() +=
            count.trim().parse::<u64>().expect("invalid count");
    }
    let total: u64 = counts.values().sum();
    assert!(total > 0, "empirical workload is empty");
    let buckets: Vec<Bucket> = counts
        .into_iter()
        .map(|(d, count)| Bucket {
            probability: count as f64 / total as f64,
            d,
        })
        .collect();
    Workload {
        name: "empirical",
        buckets: Box::leak(buckets.into_boxed_slice()),
    }
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

    if let Ok(path) = std::env::var("RECONCILE_WORKLOAD_CSV") {
        let workload = empirical_workload(&path);
        validate(workload);
        println!("Empirical session distribution loaded from {path}.");
        report("RBSR", workload, RBSR);
        report("RIBLT", workload, RIBLT);
        report("Merkle", workload, MERKLE);
    }
}
