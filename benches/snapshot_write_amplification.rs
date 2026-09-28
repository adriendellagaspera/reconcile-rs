// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Paired persistence probe: the same binary can measure the legacy single-file snapshot backend
// or the incremental base+delta backend because it observes only files created under the configured
// snapshot path. "Write bytes" means bytes in newly-created or content-changed durable files after
// one checkpoint; this intentionally excludes filesystem metadata, fsync amplification and RSS.
// Retained bytes are the complete durable footprint after the checkpoint. Durations include
// ReplicatedMap collection, serialization, file I/O, sync and publication.
//
// Run: cargo bench --bench snapshot_write_amplification
// Pair sweep:
//   RECONCILE_SNAPSHOT_SIZES=10000,100000
//   RECONCILE_SNAPSHOT_DELTAS=0,1,100,1000
// Long-chain sweep:
//   RECONCILE_SNAPSHOT_CHAIN_LENGTHS=1,8,32,128
//   RECONCILE_SNAPSHOT_CHAIN_DELTAS=1,100
// Common:
//   RECONCILE_SNAPSHOT_TRIALS=3
//
// This is a counted/timed report, deliberately not a Criterion statistical benchmark.

use std::collections::BTreeMap;
use std::fs;
use std::hint::black_box;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use reconcile::{replicated_map::Config, FileSnapshot, ReplicatedMap};
use tokio::runtime::Runtime;

const VALUE_BYTES: usize = 64;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
static NEXT_PORT: AtomicU32 = AtomicU32::new(30_000);

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileDigest {
    len: u64,
    hash: [u8; 32],
    modified: Option<SystemTime>,
}

#[derive(Clone, Debug, Default)]
struct StoreImage {
    files: BTreeMap<PathBuf, FileDigest>,
}

impl StoreImage {
    fn capture(path: &Path) -> Self {
        let mut image = Self::default();
        if path.is_file() {
            image.add_file(path);
        }

        let dir = incremental_dir(path);
        if dir.is_dir() {
            for entry in fs::read_dir(&dir).expect("read snapshot directory") {
                let path = entry.expect("read snapshot directory entry").path();
                if path.is_file() {
                    image.add_file(&path);
                }
            }
        }
        image
    }

    fn add_file(&mut self, path: &Path) {
        self.files.insert(path.to_path_buf(), digest(path));
    }

    fn retained_bytes(&self) -> u64 {
        self.files.values().map(|file| file.len).sum()
    }

    fn write_bytes_since(&self, before: &Self) -> u64 {
        self.files
            .iter()
            .filter_map(|(path, after)| {
                (before.files.get(path) != Some(after)).then_some(after.len)
            })
            .sum()
    }

    fn segments(&self, legacy_path: &Path) -> (usize, usize) {
        let mut bases = usize::from(self.files.contains_key(legacy_path));
        let mut deltas = 0;
        for path in self.files.keys() {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("base-") {
                bases += 1;
            } else if name.starts_with("delta-") {
                deltas += 1;
            }
        }
        (bases, deltas)
    }
}

#[derive(Clone, Copy, Debug)]
struct Measurement {
    reference_time: Duration,
    checkpoint_time: Duration,
    reference_write_bytes: u64,
    checkpoint_write_bytes: u64,
    retained_bytes: u64,
    max_retained_bytes: u64,
    base_segments: usize,
    delta_segments: usize,
    max_delta_segments: usize,
    restart_time: Duration,
}

fn incremental_dir(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".d");
    PathBuf::from(value)
}

fn digest(path: &Path) -> FileDigest {
    let mut file = fs::File::open(path).expect("open snapshot file");
    let metadata = file.metadata().expect("stat snapshot file");
    let len = metadata.len();
    let modified = metadata.modified().ok();
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; HASH_BUFFER_BYTES];
    loop {
        let n = file.read(&mut buffer).expect("read snapshot file");
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    FileDigest {
        len,
        hash: *hasher.finalize().as_bytes(),
        modified,
    }
}

fn values(name: &str, default: &str) -> Vec<usize> {
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

fn make_store(rt: &Runtime, path: &Path) -> ReplicatedMap<u32, Vec<u8>> {
    rt.block_on(ReplicatedMap::new(config()))
        .expect("create a peerless map")
        .with_persistence(Arc::new(FileSnapshot::new(path)))
        .expect("load the existing snapshot, if any")
}

fn reference_snapshot(
    store: &ReplicatedMap<u32, Vec<u8>>,
    path: &Path,
) -> (Duration, StoreImage, u64) {
    let before = StoreImage::capture(path);
    let start = Instant::now();
    store.snapshot_now().expect("write reference snapshot");
    let elapsed = start.elapsed();
    let after = StoreImage::capture(path);
    let written = after.write_bytes_since(&before);
    (elapsed, after, written)
}

fn mutate(store: &ReplicatedMap<u32, Vec<u8>>, n: usize, delta: usize, generation: usize) {
    if delta == 0 {
        return;
    }
    let start = generation.saturating_mul(delta) % n;
    let updates: Vec<_> = (0..delta)
        .map(|offset| {
            let key = ((start + offset) % n) as u32;
            let byte = ((generation % 251) + 1) as u8;
            (key, vec![byte; VALUE_BYTES])
        })
        .collect();
    store.load_bulk(&updates);
}

fn verify_restart(
    rt: &Runtime,
    path: &Path,
    source: &ReplicatedMap<u32, Vec<u8>>,
    n: usize,
) -> Duration {
    let expected = black_box(source.fingerprint(..));
    let start = Instant::now();
    let restarted = make_store(rt, path);
    let elapsed = start.elapsed();
    assert_eq!(
        restarted.fingerprint(..),
        expected,
        "restart changed the dated map"
    );
    assert_eq!(restarted.snapshot().len(), n, "restart lost an entry");
    elapsed
}

fn trial(rt: &Runtime, n: usize, delta: usize, checkpoints: usize) -> Measurement {
    assert!(n > 0 && n <= u32::MAX as usize);
    assert!(delta <= n);
    assert!(checkpoints > 0);

    let dir = tempfile::tempdir().expect("create benchmark directory");
    let path = dir.path().join("snapshot.bin");
    let store = make_store(rt, &path);

    let initial: Vec<_> = (0..n as u32).map(|key| (key, vec![0; VALUE_BYTES])).collect();
    store.load_bulk(&initial);
    let (reference_time, mut before, reference_write_bytes) =
        reference_snapshot(&store, &path);

    let mut checkpoint_time = Duration::ZERO;
    let mut checkpoint_write_bytes = 0;
    let mut max_retained_bytes = before.retained_bytes();
    let (_, initial_delta_segments) = before.segments(&path);
    let mut max_delta_segments = initial_delta_segments;
    for generation in 1..=checkpoints {
        mutate(&store, n, delta, generation);
        let start = Instant::now();
        store.snapshot_now().expect("write measured checkpoint");
        checkpoint_time += start.elapsed();

        let after = StoreImage::capture(&path);
        checkpoint_write_bytes += after.write_bytes_since(&before);
        max_retained_bytes = max_retained_bytes.max(after.retained_bytes());
        let (_, current_delta_segments) = after.segments(&path);
        max_delta_segments = max_delta_segments.max(current_delta_segments);
        before = after;
    }

    let retained_bytes = before.retained_bytes();
    let (base_segments, delta_segments) = before.segments(&path);
    let restart_time = verify_restart(rt, &path, &store, n);

    Measurement {
        reference_time,
        checkpoint_time,
        reference_write_bytes,
        checkpoint_write_bytes,
        retained_bytes,
        max_retained_bytes,
        base_segments,
        delta_segments,
        max_delta_segments,
        restart_time,
    }
}

fn print_measurement(
    case: &str,
    n: usize,
    delta: usize,
    checkpoints: usize,
    repetition: usize,
    m: Measurement,
) {
    let full_baseline = m.reference_write_bytes.saturating_mul(checkpoints as u64);
    let write_ratio = if full_baseline == 0 {
        0.0
    } else {
        m.checkpoint_write_bytes as f64 / full_baseline as f64
    };
    println!(
        "{case},{n},{delta},{checkpoints},{repetition},{:.3},{:.3},{},{},{:.6},{},{},{},{},{},{:.3}",
        m.reference_time.as_secs_f64() * 1_000.0,
        m.checkpoint_time.as_secs_f64() * 1_000.0,
        m.reference_write_bytes,
        m.checkpoint_write_bytes,
        write_ratio,
        m.retained_bytes,
        m.max_retained_bytes,
        m.base_segments,
        m.delta_segments,
        m.max_delta_segments,
        m.restart_time.as_secs_f64() * 1_000.0,
    );
}

fn main() {
    let rt = Runtime::new().expect("create benchmark runtime");
    let ns = values("RECONCILE_SNAPSHOT_SIZES", "10000,100000");
    let deltas = values("RECONCILE_SNAPSHOT_DELTAS", "0,1,100,1000");
    let chain_lengths = values("RECONCILE_SNAPSHOT_CHAIN_LENGTHS", "1,8,32");
    let chain_deltas = values("RECONCILE_SNAPSHOT_CHAIN_DELTAS", "1,100");
    let repeats = std::env::var("RECONCILE_SNAPSHOT_TRIALS")
        .unwrap_or_else(|_| "3".to_owned())
        .parse::<usize>()
        .expect("invalid trial count");
    assert!(repeats > 0);

    println!(
        "case,n,delta,checkpoints,trial,reference_ms,checkpoint_total_ms,reference_write_bytes,checkpoint_write_bytes,write_ratio_to_full_baseline,retained_bytes,max_retained_bytes,base_segments,delta_segments,max_delta_segments,restart_ms"
    );

    for n in ns {
        for &delta in &deltas {
            if delta > n {
                continue;
            }
            for repetition in 0..repeats {
                print_measurement(
                    "pair",
                    n,
                    delta,
                    1,
                    repetition,
                    trial(&rt, n, delta, 1),
                );
            }
        }

        for &delta in &chain_deltas {
            if delta > n {
                continue;
            }
            for &checkpoints in &chain_lengths {
                if checkpoints == 0 {
                    continue;
                }
                for repetition in 0..repeats {
                    print_measurement(
                        "chain",
                        n,
                        delta,
                        checkpoints,
                        repetition,
                        trial(&rt, n, delta, checkpoints),
                    );
                }
            }
        }
    }
}
