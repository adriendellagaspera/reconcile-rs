// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #46 baseline: logical bytes written by the *current* full-snapshot implementation when
//! only d of n keys change. This is a manual, single-process, peerless probe: no periodic
//! task, gossip traffic or concurrent writers. Each trial uses a fresh directory and store.
//!
//! "Bytes" is the exact length of the encoded snapshot file, i.e. payload bytes passed to
//! the file writer (not physical-device write amplification, filesystem metadata or RSS).
//! Durations include ReplicatedMap's full-state collection, serialization, file sync and
//! rename; they are not isolated disk throughput. An explicit snapshot_now() is issued
//! even at d=0 to expose its full-rewrite semantics; the periodic idle threshold is separate.
//! A fresh process-equivalent store loads each completed snapshot and must match its source.
//!
//! Run: cargo bench --bench snapshot_write_amplification
//! Override: RECONCILE_BASELINE_SIZES=10000,100000,1000000
//!           RECONCILE_BASELINE_DELTAS=0,1,100,1000
//!           RECONCILE_BASELINE_TRIALS=3
//! This is a counted/timed report, deliberately not a Criterion statistical benchmark.

use std::fs;
use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reconcile::{replicated_map::Config, FileSnapshot, ReplicatedMap};
use tokio::runtime::Runtime;

const VALUE_BYTES: usize = 64;
static NEXT_PORT: AtomicU32 = AtomicU32::new(30_000);

fn sizes(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<usize>()
                .expect("invalid comma-separated nonnegative integer")
        })
        .collect()
}

fn config() -> Config {
    let port = 30_000 + NEXT_PORT.fetch_add(1, Ordering::Relaxed) % 20_000;
    Config::new(port as u16)
        .with_listen_addr("127.0.0.1".parse().unwrap())
        .with_net("127.0.0.1/8".parse().unwrap())
        .unwrap()
        .with_insecure_no_key()
        .with_snapshot_interval(None)
}

fn make_store(rt: &Runtime, path: &std::path::Path) -> ReplicatedMap<u32, Vec<u8>> {
    rt.block_on(ReplicatedMap::new(config()))
        .expect("create a peerless map")
        .with_persistence(Arc::new(FileSnapshot::new(path)))
        .expect("load the existing snapshot, if any")
}

fn trial(rt: &Runtime, n: usize, delta: usize) -> (Duration, Duration, u64, u64, Duration) {
    assert!(n > 0 && n <= u32::MAX as usize);
    assert!(delta <= n);
    let dir = tempfile::tempdir().expect("create benchmark directory");
    let path = dir.path().join("snapshot.bin");
    let store = make_store(rt, &path);

    // Corpus is constructed outside both timed snapshot windows.
    let initial: Vec<_> = (0..n as u32).map(|k| (k, vec![0; VALUE_BYTES])).collect();
    store.load_bulk(&initial);
    let start = Instant::now();
    store.snapshot_now().expect("write reference snapshot");
    let initial_time = start.elapsed();
    let initial_bytes = fs::metadata(&path).unwrap().len();

    let updates: Vec<_> = (0..delta as u32)
        .map(|k| (k, vec![7; VALUE_BYTES]))
        .collect();
    store.load_bulk(&updates);
    let start = Instant::now();
    store.snapshot_now().expect("write second snapshot");
    let second_time = start.elapsed();
    let second_bytes = fs::metadata(&path).unwrap().len();

    let expected = black_box(store.fingerprint(..));
    let start = Instant::now();
    let restarted = make_store(rt, &path);
    let restart_time = start.elapsed();
    assert_eq!(
        restarted.fingerprint(..),
        expected,
        "restart changed the dated map"
    );
    assert_eq!(restarted.snapshot().len(), n, "restart lost an entry");
    if delta > 0 {
        assert_eq!(restarted.get_cloned(&0), Some(vec![7; VALUE_BYTES]));
    }
    assert_eq!(
        restarted.get_cloned(&(n as u32 - 1)),
        Some(vec![if delta == n { 7 } else { 0 }; VALUE_BYTES])
    );

    (
        initial_time,
        second_time,
        initial_bytes,
        second_bytes,
        restart_time,
    )
}

fn main() {
    let rt = Runtime::new().expect("create benchmark runtime");
    let ns = sizes("RECONCILE_BASELINE_SIZES", "10000,100000");
    let deltas = sizes("RECONCILE_BASELINE_DELTAS", "0,1,100,1000");
    let repeats = std::env::var("RECONCILE_BASELINE_TRIALS")
        .unwrap_or_else(|_| "3".to_owned())
        .parse::<usize>()
        .expect("invalid trial count");
    assert!(repeats > 0);
    println!("n,delta,trial,reference_ms,rewrite_ms,reference_bytes,rewrite_bytes,rewrite_to_reference_ratio,restart_ms");
    for n in ns {
        for &delta in &deltas {
            if delta > n {
                continue;
            }
            for repetition in 0..repeats {
                let (first, second, reference_bytes, rewrite_bytes, restart) = trial(&rt, n, delta);
                println!(
                    "{n},{delta},{repetition},{:.3},{:.3},{reference_bytes},{rewrite_bytes},{:.6},{:.3}",
                    first.as_secs_f64() * 1_000.0,
                    second.as_secs_f64() * 1_000.0,
                    rewrite_bytes as f64 / reference_bytes as f64,
                    restart.as_secs_f64() * 1_000.0
                );
            }
        }
    }
}
