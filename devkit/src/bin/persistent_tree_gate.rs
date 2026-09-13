#![forbid(unsafe_code)]

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_ITERS: usize = 10_000;

fn tree(n: usize) -> FingerprintTreeMap<u32, u32> {
    let mut tree = FingerprintTreeMap::new();
    for k in 0..n as u32 {
        tree.insert(k, k.wrapping_mul(2_654_435_761));
    }
    tree
}

fn retained_history(
    mut current: FingerprintTreeMap<u32, u32>,
    count: usize,
) -> (FingerprintTreeMap<u32, u32>, Vec<FingerprintTreeMap<u32, u32>>) {
    if count == 0 {
        return (current, Vec::new());
    }

    let mut retained = Vec::with_capacity(count);
    for i in 0..count.saturating_sub(1) {
        retained.push(current.clone());
        let key = (i % current.len()) as u32;
        current.insert(key, key.wrapping_add(i as u32).wrapping_add(1));
    }
    retained.push(current.clone());
    (current, retained)
}

fn ns_per_op(elapsed: Duration, iters: usize) -> f64 {
    elapsed.as_nanos() as f64 / iters as f64
}

fn run_snapshot_acquire(n: usize, iters: usize) {
    let tree = tree(n);
    let start = Instant::now();
    for _ in 0..iters {
        black_box(tree.clone());
    }
    println!(
        "snapshot_acquire,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );
}

fn run_point_read(n: usize, iters: usize) {
    let tree = tree(n);
    let key = (n / 2) as u32;
    let start = Instant::now();
    for _ in 0..iters {
        black_box(tree.get(black_box(&key)));
    }
    println!(
        "point_read,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );
}

fn run_iteration(n: usize, iters: usize) {
    let tree = tree(n);
    let start = Instant::now();
    for _ in 0..iters {
        let sum = tree.iter().fold(0u64, |acc, (k, v)| {
            acc.wrapping_add(*k as u64).wrapping_add(*v as u64)
        });
        black_box(sum);
    }
    println!(
        "iteration,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );
}

fn run_range_iteration(n: usize, iters: usize) {
    let tree = tree(n);
    let lo = (n / 4) as u32;
    let hi = (3 * n / 4) as u32;
    let start = Instant::now();
    for _ in 0..iters {
        let sum = tree.range(lo..hi).fold(0u64, |acc, (k, v)| {
            acc.wrapping_add(*k as u64).wrapping_add(*v as u64)
        });
        black_box(sum);
    }
    println!(
        "range_iteration,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );
}

fn run_mutation(n: usize, retained_count: usize, iters: usize) {
    let base = tree(n);
    let key = (n / 2) as u32;
    let mut elapsed = Duration::ZERO;

    for i in 0..iters {
        // Setup and teardown are deliberately outside the timed interval. The question is the
        // write-path cost once this many historical versions are alive, not the cost of creating
        // or dropping those versions.
        let (mut current, retained) = retained_history(base.clone(), retained_count);
        let old = *current.get(&key).expect("key exists");
        let start = Instant::now();
        current.insert(key, old.wrapping_add(i as u32).wrapping_add(1));
        elapsed += start.elapsed();
        black_box((&current, &retained));
    }

    println!(
        "mutation_retained,n={n},retained={retained_count},iters={iters},ns_per_op={:.2}",
        ns_per_op(elapsed, iters)
    );
}

fn hold_for_rss(n: usize, retained_count: usize) {
    let base = tree(n);
    let (current, retained) = retained_history(base, retained_count);
    println!(
        "rss_hold,n={n},retained={retained_count},current_len={},versions_held={}",
        current.len(),
        retained.len()
    );
    black_box((&current, &retained));
}

fn parse_usize(args: &[String], flag: &str, default: usize) -> usize {
    args.windows(2)
        .find(|w| w[0] == flag)
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("all");
    let n = parse_usize(&args, "--n", DEFAULT_N);
    let iters = parse_usize(&args, "--iters", DEFAULT_ITERS);
    let retained = parse_usize(&args, "--retained", 1);

    match mode {
        "snapshot" => run_snapshot_acquire(n, iters),
        "point-read" => run_point_read(n, iters),
        "iteration" => run_iteration(n, iters),
        "range" => run_range_iteration(n, iters),
        "mutation" => run_mutation(n, retained, iters),
        "rss-hold" => hold_for_rss(n, retained),
        "all" => {
            run_snapshot_acquire(n, iters);
            run_point_read(n, iters);
            run_iteration(n, 100.max(iters / 100));
            run_range_iteration(n, 100.max(iters / 100));
            for retained_count in [0, 1, 8, 64] {
                run_mutation(n, retained_count, 100.max(iters / 100));
            }
        }
        _ => panic!("unknown mode: {mode}"),
    }
}
