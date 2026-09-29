// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Metadata-only persistence probe.
//
// Builds a durable state with one tombstone plus a large authoritative membership/ACK map, then
// removes peers through the public forget_peer path without changing any entry value. The same
// benchmark source runs against full-snapshot and incremental revisions.
//
// Run:
//   cargo bench --bench snapshot_metadata_only
//
// Overrides:
//   RECONCILE_METADATA_MEMBERS=10000
//   RECONCILE_METADATA_BATCHES=1,10,100,1000
//   RECONCILE_METADATA_CHAIN_LENGTHS=1,8,32,128
//   RECONCILE_METADATA_TRIALS=1

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use reconcile::{
    replicated_map::Config, Entry, FileSnapshot, Hlc, LogicalCounter, NodeId, PersistedState,
    Persistence, PhysicalTime, ReplicatedMap, Timestamp,
};
use tokio::runtime::Runtime;

const PORT_BASE: u32 = 41_000;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
static NEXT_PORT: AtomicU32 = AtomicU32::new(PORT_BASE);

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileDigest {
    len: u64,
    hash: [u8; 32],
    modified: Option<SystemTime>,
}

#[derive(Clone, Debug, Default)]
struct StoreImage {
    files: HashMap<PathBuf, FileDigest>,
}

impl StoreImage {
    fn capture(path: &Path) -> Self {
        let mut image = Self::default();
        if path.is_file() {
            image.add_file(path);
        }
        let mut dir_os = path.as_os_str().to_os_string();
        dir_os.push(".d");
        let dir = PathBuf::from(dir_os);
        if dir.is_dir() {
            for entry in fs::read_dir(dir).expect("read snapshot directory") {
                let path = entry.expect("read snapshot entry").path();
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

    fn write_bytes_since(&self, before: &Self) -> u64 {
        self.files
            .iter()
            .filter_map(|(path, after)| {
                (before.files.get(path) != Some(after)).then_some(after.len)
            })
            .sum()
    }

    fn retained_bytes(&self) -> u64 {
        self.files.values().map(|file| file.len).sum()
    }

    fn segment_counts(&self, legacy_path: &Path) -> (usize, usize) {
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

fn digest(path: &Path) -> FileDigest {
    let mut file = fs::File::open(path).expect("open snapshot file");
    let metadata = file.metadata().expect("stat snapshot file");
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
        len: metadata.len(),
        hash: *hasher.finalize().as_bytes(),
        modified: metadata.modified().ok(),
    }
}

fn values(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|part| part.trim().parse::<usize>().expect("invalid integer list"))
        .collect()
}

fn peer_ip(index: usize) -> IpAddr {
    assert!(index < 0x00ff_fffe);
    IpAddr::V4(Ipv4Addr::from(0x0a00_0001u32 + index as u32))
}

fn config() -> Config {
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed) as u16;
    Config::new(port)
        .with_listen_addr("127.0.0.1".parse().unwrap())
        .with_node_id(NodeId::new(1))
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn initial_state(members: usize) -> PersistedState<u64, u64> {
    let peers: HashSet<_> = (0..members).map(peer_ip).collect();
    let tombstone = Entry::tombstone(Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(1), LogicalCounter::ZERO),
        NodeId::new(1),
    ));
    let acks = peers.iter().copied().map(|peer| (peer, 1)).collect();
    PersistedState::new(vec![(0, tombstone)], peers, HashMap::from([(0, acks)]))
}

fn make_store(
    rt: &Runtime,
    path: &Path,
    state: &PersistedState<u64, u64>,
) -> ReplicatedMap<u64, u64> {
    let backend = Arc::new(FileSnapshot::new(path));
    Persistence::<u64, u64>::save(&*backend, state).expect("seed durable metadata state");
    rt.block_on(ReplicatedMap::new(config()))
        .expect("create replica")
        .with_persistence(backend)
        .expect("load seeded state")
}

fn trial(
    rt: &Runtime,
    members: usize,
    batch: usize,
    checkpoints: usize,
) -> (Duration, u64, u64, u64, usize, usize, usize, Duration) {
    assert!(batch > 0);
    assert!(batch.saturating_mul(checkpoints) <= members);

    let dir = tempfile::tempdir().expect("benchmark directory");
    let path = dir.path().join("snapshot.bin");
    let state = initial_state(members);
    let store = make_store(rt, &path, &state);
    let mut before = StoreImage::capture(&path);
    let mut max_retained = before.retained_bytes();
    let (_, initial_deltas) = before.segment_counts(&path);
    let mut max_delta_segments = initial_deltas;

    let start = Instant::now();
    let mut written = 0;
    for generation in 0..checkpoints {
        let begin = generation * batch;
        for index in begin..begin + batch {
            store.forget_peer(peer_ip(index));
        }
        store.snapshot_now().expect("persist metadata-only generation");
        let after = StoreImage::capture(&path);
        written += after.write_bytes_since(&before);
        max_retained = max_retained.max(after.retained_bytes());
        let (_, current_deltas) = after.segment_counts(&path);
        max_delta_segments = max_delta_segments.max(current_deltas);
        before = after;
    }
    let checkpoint_time = start.elapsed();

    let retained = before.retained_bytes();
    let (bases, deltas) = before.segment_counts(&path);
    let expected_members = members - batch * checkpoints;

    let restart_start = Instant::now();
    let restarted = rt
        .block_on(ReplicatedMap::<u64, u64>::new(config()))
        .expect("create restart replica")
        .with_persistence(Arc::new(FileSnapshot::new(&path)))
        .expect("reload metadata-only chain");
    let restart = restart_start.elapsed();
    assert_eq!(restarted.members().len(), expected_members);
    assert_eq!(restarted.snapshot().len(), 1);

    (
        checkpoint_time,
        written,
        retained,
        max_retained,
        bases,
        deltas,
        max_delta_segments,
        restart,
    )
}

fn main() {
    let rt = Runtime::new().expect("Tokio runtime");
    let members = std::env::var("RECONCILE_METADATA_MEMBERS")
        .unwrap_or_else(|_| "10000".to_owned())
        .parse::<usize>()
        .expect("invalid membership size");
    let batches = values("RECONCILE_METADATA_BATCHES", "1,10,100,1000");
    let chains = values("RECONCILE_METADATA_CHAIN_LENGTHS", "1,8,32,128");
    let trials = std::env::var("RECONCILE_METADATA_TRIALS")
        .unwrap_or_else(|_| "1".to_owned())
        .parse::<usize>()
        .expect("invalid trial count");

    println!(
        "members,batch,checkpoints,trial,checkpoint_total_ms,checkpoint_write_bytes,retained_bytes,max_retained_bytes,base_segments,delta_segments,max_delta_segments,restart_ms"
    );

    for batch in batches {
        for &checkpoints in &chains {
            if batch.saturating_mul(checkpoints) > members {
                continue;
            }
            for trial_index in 0..trials {
                let (
                    elapsed,
                    written,
                    retained,
                    max_retained,
                    bases,
                    deltas,
                    max_deltas,
                    restart,
                ) = trial(&rt, members, batch, checkpoints);
                println!(
                    "{members},{batch},{checkpoints},{trial_index},{:.3},{written},{retained},{max_retained},{bases},{deltas},{max_deltas},{:.3}",
                    elapsed.as_secs_f64() * 1_000.0,
                    restart.as_secs_f64() * 1_000.0
                );
            }
        }
    }
}
