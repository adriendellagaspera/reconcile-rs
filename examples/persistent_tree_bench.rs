// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Persistent-tree gate for #29.
//!
//! The historical mutate-in-place comparator is commit
//! `0dbc656e8f7e2ed4c978a5f46bb8364c01688304`; `benches/README.md` owns the exact two-worktree
//! reproduction commands. This example exists because #29 is a one-off architecture gate rather
//! than a fifth permanent benchmark family: it measures COW-specific costs that the historical
//! commit cannot express, while the existing `bench`/`system` targets remain the long-lived
//! regression suite.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput};
use rsos::FingerprintTreeMap;

const SIZES: &[usize] = &[1_000, 10_000, 100_000];
const RETAINED: &[usize] = &[0, 1, 8, 64];

struct CountingAllocator;
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn tree(n: usize) -> FingerprintTreeMap<u32, u32> {
    let mut tree = FingerprintTreeMap::new();
    for k in 0..n as u32 {
        tree.insert(k, k.wrapping_mul(2_654_435_761));
    }
    tree
}

/// Retain `count` successive historical versions plus, when `count > 0`, the exact current
/// version. The latter is what makes the next mutation exercise `Arc::make_mut`'s path copy;
/// older versions add the memory pressure a long-lived snapshot workload creates.
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

fn snapshot_acquire(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_tree/snapshot_acquire");
    for &n in SIZES {
        let tree = tree(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(tree.clone()));
        });
    }
    group.finish();
}

fn point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_tree/point_read");
    for &n in SIZES {
        let tree = tree(n);
        let key = (n / 2) as u32;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(tree.get(black_box(&key))));
        });
    }
    group.finish();
}

fn iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_tree/iteration");
    for &n in SIZES {
        let tree = tree(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let sum = tree.iter().fold(0u64, |acc, (k, v)| {
                    acc.wrapping_add(*k as u64).wrapping_add(*v as u64)
                });
                black_box(sum)
            });
        });
    }
    group.finish();
}

fn range_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_tree/range_iteration");
    for &n in SIZES {
        let tree = tree(n);
        let lo = (n / 4) as u32;
        let hi = (3 * n / 4) as u32;
        group.throughput(Throughput::Elements((hi - lo) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let sum = tree.range(lo..hi).fold(0u64, |acc, (k, v)| {
                    acc.wrapping_add(*k as u64).wrapping_add(*v as u64)
                });
                black_box(sum)
            });
        });
    }
    group.finish();
}

fn mutation_with_retained_snapshots(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistent_tree/mutation_retained");
    group.sample_size(30);
    group.sampling_mode(SamplingMode::Flat);

    for &n in SIZES {
        let base = tree(n);
        for &retained_count in RETAINED {
            let id = BenchmarkId::new(format!("retained={retained_count}"), n);
            group.bench_with_input(id, &(n, retained_count), |b, &(_, retained_count)| {
                b.iter_batched(
                    || retained_history(base.clone(), retained_count),
                    |(mut current, retained)| {
                        let key = (n / 2) as u32;
                        let old = *current.get(&key).expect("key exists");
                        current.insert(key, old.wrapping_add(1));
                        current.remove(&key);
                        current.insert(key, old);
                        black_box((current, retained));
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

/// Prints requested live-heap growth from retaining successive COW versions. This mirrors
/// `system::heap_footprint`: it is a floor on RSS, excluding allocator rounding and fragmentation,
/// and exists to compare retained-version shapes rather than claim exact RSS.
fn retained_snapshot_memory(c: &mut Criterion) {
    for &n in SIZES {
        let base = tree(n);
        for &retained_count in RETAINED.iter().filter(|&&count| count > 0) {
            let before = LIVE_BYTES.load(Ordering::Relaxed);
            let (current, retained) = retained_history(base.clone(), retained_count);
            let after = LIVE_BYTES.load(Ordering::Relaxed);
            let delta = after - before;
            println!(
                "[persistent_tree_memory] n={n}, retained={retained_count}: {delta} B live-heap growth, {:.1} B/retained-version",
                delta as f64 / retained_count as f64
            );
            black_box((&current, &retained));
            drop((current, retained));
        }
    }

    c.bench_function("persistent_tree/retained_memory_counter", |b| {
        b.iter(|| black_box(LIVE_BYTES.load(Ordering::Relaxed)));
    });
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    snapshot_acquire(&mut criterion);
    point_read(&mut criterion);
    iteration(&mut criterion);
    range_iteration(&mut criterion);
    mutation_with_retained_snapshots(&mut criterion);
    retained_snapshot_memory(&mut criterion);
    criterion.final_summary();
}
