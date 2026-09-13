#![forbid(unsafe_code)]

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_ITERS: usize = 100_000;

fn tree(n: usize) -> FingerprintTreeMap<u32, u32> {
    let mut tree = FingerprintTreeMap::new();
    for k in 0..n as u32 {
        tree.insert(k, k.wrapping_mul(2_654_435_761));
    }
    tree
}

fn ns_per_op(elapsed: Duration, iters: usize) -> f64 {
    elapsed.as_nanos() as f64 / iters as f64
}

fn parse_usize(args: &[String], flag: &str, default: usize) -> usize {
    args.windows(2)
        .find(|w| w[0] == flag)
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let n = parse_usize(&args, "--n", DEFAULT_N);
    let iters = parse_usize(&args, "--iters", DEFAULT_ITERS);
    assert!(n >= 100);
    assert!(iters > 0);

    // Fill is intentionally a small repeated sample: the work is O(n), unlike every operation
    // below. Building outside a Criterion harness keeps this exact source compilable against the
    // pre-COW commit as well as the candidate.
    let fill_iters = 5usize;
    let start = Instant::now();
    for _ in 0..fill_iters {
        black_box(tree(n));
    }
    println!(
        "fill,n={n},iters={fill_iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), fill_iters)
    );

    let mut tree = tree(n);
    let probe = (n / 2) as u32;

    let start = Instant::now();
    for _ in 0..iters {
        black_box(tree.get(black_box(&probe)));
    }
    println!(
        "point_read,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );

    // Fixed-width moving ranges exercise the same cached-summary descent on both revisions. The
    // arithmetic is deliberately outside the timed aggregate call only in conceptual terms; its
    // tiny, identical cost is included in both arms and avoids allocating a corpus of ranges.
    let width = (n / 10) as u32;
    let max_start = n as u32 - width;
    let start = Instant::now();
    for i in 0..iters {
        let lo = ((i as u32).wrapping_mul(7_919)) % max_start;
        black_box(tree.aggregate(lo..lo + width));
    }
    println!(
        "aggregate,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );

    // Overwrite an existing key and alternate the value so the compiler cannot collapse the
    // writes. This isolates the common root-path maintenance cost without structural rebalancing.
    let start = Instant::now();
    for i in 0..iters {
        black_box(tree.insert(probe, i as u32));
    }
    println!(
        "overwrite,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );

    // Insert then remove one absent key. Reporting the pair avoids timer noise around two
    // sub-microsecond calls while still pricing the structural mutation path identically.
    let absent = n as u32 + 1;
    let start = Instant::now();
    for i in 0..iters {
        black_box(tree.insert(absent, i as u32));
        black_box(tree.remove(&absent));
    }
    println!(
        "insert_remove_pair,n={n},iters={iters},ns_per_op={:.2}",
        ns_per_op(start.elapsed(), iters)
    );
}
