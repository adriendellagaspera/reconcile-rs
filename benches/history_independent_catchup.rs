// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// RBSR catch-up cost against superseded mutation history.
//
// The current states are held constant while `h`, the number of writes that happened and were
// later superseded during a partition, varies. History construction and normalization happen before
// the timed section. The benchmark asserts both current states and the exact protocol-cost trace are
// identical across every `h`; Criterion then times only reconciliation of those current states.
//
// Defaults:
//   n = 100_000 keys
//   d = 100 final divergent keys
//   h = 0, 1_000, 100_000 superseded writes
//
// Overrides:
//   RECONCILE_HISTORY_N=1000000
//   RECONCILE_HISTORY_D=1000
//   RECONCILE_HISTORY_OPS=0,1000,100000,1000000
//
// Run with `cargo bench --bench history_independent_catchup`.

use std::env;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use devkit::protocol_cost::{reconcile, Cost, Counting, Queries};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut, RefinementPolicy};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: usize = 100;
const DEFAULT_HISTORY_OPS: &[usize] = &[0, 1_000, 100_000];
const CHURN_KEYS: usize = 64;
const SESSION_SEED: u64 = 42;

struct Corpus {
    history_ops: usize,
    left: FingerprintTreeMap<u64, u64>,
    right: FingerprintTreeMap<u64, u64>,
}

#[derive(Debug, Eq, PartialEq)]
struct CostSignature {
    messages: usize,
    ranges: usize,
    refinement_bytes: usize,
    datagrams: usize,
    fragments: usize,
    largest_message: usize,
    largest_message_bytes: usize,
    enumerations: usize,
    enumerated_elements: usize,
    enumerated_bytes: Vec<usize>,
    queries: Queries,
}

impl From<&Cost> for CostSignature {
    fn from(cost: &Cost) -> Self {
        Self {
            messages: cost.messages,
            ranges: cost.ranges,
            refinement_bytes: cost.refinement_bytes,
            datagrams: cost.datagrams,
            fragments: cost.fragments,
            largest_message: cost.largest_message,
            largest_message_bytes: cost.largest_message_bytes,
            enumerations: cost.enumerations,
            enumerated_elements: cost.enumerated_elements,
            enumerated_bytes: cost.enumerated_bytes.clone(),
            queries: cost.queries,
        }
    }
}

fn base_value(key: u64) -> u64 {
    key.wrapping_mul(2_654_435_761)
}

fn store(n: usize) -> FingerprintTreeMap<u64, u64> {
    let mut map = FingerprintTreeMap::new();
    for key in 0..n as u64 {
        map.insert(key, base_value(key));
    }
    map
}

// Apply `history_ops` writes that deliberately leave no information in the final state. Writes are
// spread over a small hot set and across both replicas, then a fixed normalization pass restores
// that hot set. The normalization writes are harness bookkeeping and are not counted in `h`.
fn apply_superseded_history(
    left: &mut FingerprintTreeMap<u64, u64>,
    right: &mut FingerprintTreeMap<u64, u64>,
    history_ops: usize,
) {
    for op in 0..history_ops {
        let key = (op % CHURN_KEYS) as u64;
        let transient = base_value(key)
            .wrapping_add(op as u64 + 1)
            .rotate_left((op % 63) as u32);
        if op % 2 == 0 {
            left.insert(key, transient);
        } else {
            right.insert(key, transient);
        }
    }

    for key in 0..CHURN_KEYS as u64 {
        let value = base_value(key);
        left.insert(key, value);
        right.insert(key, value);
    }
}

fn divergence_keys(n: usize, d: usize) -> Vec<u64> {
    let available = n
        .checked_sub(CHURN_KEYS)
        .expect("RECONCILE_HISTORY_N must exceed the churn-key count");
    assert!(
        d < available,
        "RECONCILE_HISTORY_D must leave room for distinct keys"
    );
    let stride = available / (d + 1);
    assert!(stride > 0, "divergence-key stride must make progress");

    (1..=d)
        .map(|i| (CHURN_KEYS + stride * i) as u64)
        .collect()
}

fn build_corpus(n: usize, d: usize, history_ops: usize) -> Corpus {
    let baseline = store(n);
    let mut left = baseline.clone();
    let mut right = baseline;

    apply_superseded_history(&mut left, &mut right, history_ops);
    for key in divergence_keys(n, d) {
        right.insert(key, base_value(key) ^ u64::MAX);
    }

    Corpus {
        history_ops,
        left,
        right,
    }
}

fn exact_state_eq(
    a: &FingerprintTreeMap<u64, u64>,
    b: &FingerprintTreeMap<u64, u64>,
) -> bool {
    a.iter().eq(b.iter())
}

fn counted_reconcile(
    left: &FingerprintTreeMap<u64, u64>,
    right: &FingerprintTreeMap<u64, u64>,
    policy: &dyn RefinementPolicy,
) -> Cost {
    let (counted_left, counted_right) = (Counting::new(left), Counting::new(right));
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let mut cost = reconcile(&counted_left, &counted_right, policy, None, &mut rng);
    cost.queries = counted_left.queries() + counted_right.queries();
    cost
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
    })
}

fn history_sizes() -> Vec<usize> {
    env::var("RECONCILE_HISTORY_OPS").map_or_else(
        |_| DEFAULT_HISTORY_OPS.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!("RECONCILE_HISTORY_OPS must be a comma-separated list of integers")
                    })
                })
                .collect()
        },
    )
}

fn history_independent_catchup(c: &mut Criterion) {
    let n = env_usize("RECONCILE_HISTORY_N", DEFAULT_N);
    let d = env_usize("RECONCILE_HISTORY_D", DEFAULT_D);
    let histories = history_sizes();
    assert!(!histories.is_empty(), "at least one history size is required");

    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let reference = build_corpus(n, d, 0);
    let reference_cost = counted_reconcile(&reference.left, &reference.right, &policy);
    let reference_signature = CostSignature::from(&reference_cost);

    println!(
        "[history-catchup] n={n} d={d}; timed section excludes history construction and normalization"
    );

    let mut corpora = Vec::with_capacity(histories.len());
    for history_ops in histories {
        let corpus = build_corpus(n, d, history_ops);
        assert!(
            exact_state_eq(&reference.left, &corpus.left)
                && exact_state_eq(&reference.right, &corpus.right),
            "h={history_ops}: current states differ from h=0; the benchmark does not isolate history"
        );

        let cost = counted_reconcile(&corpus.left, &corpus.right, &policy);
        let signature = CostSignature::from(&cost);
        assert_eq!(
            signature, reference_signature,
            "h={history_ops}: identical current states produced a different RBSR trace"
        );

        println!(
            "[history-catchup] h={history_ops:>10} | refine={:>8} B | messages={:>3} | ranges={:>6} | idlist={:>5} elem | agg={:>6} rank={:>6} select={:>6}",
            cost.refinement_bytes,
            cost.messages,
            cost.ranges,
            cost.enumerated_elements,
            cost.queries.aggregate,
            cost.queries.rank,
            cost.queries.select,
        );
        corpora.push(corpus);
    }

    let mut group = c.benchmark_group("history_independent_catchup");
    group.sample_size(20);
    for corpus in &corpora {
        group.bench_with_input(
            BenchmarkId::new("history_ops", corpus.history_ops),
            corpus,
            |bencher, corpus| {
                bencher.iter(|| {
                    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
                    black_box(reconcile(
                        black_box(&corpus.left),
                        black_box(&corpus.right),
                        &policy,
                        None,
                        &mut rng,
                    ))
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, history_independent_catchup);
criterion_main!(benches);
