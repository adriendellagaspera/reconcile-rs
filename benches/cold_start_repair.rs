// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Cold-start state-repair benchmark.
//
// Phase A discards reconciliation acceleration state while canonical rows are already in memory.
// Phase B persists the same current state through FileSnapshot, then measures load/deserialization,
// live-row projection, index/sketch reconstruction and the same repair cost. Snapshot creation is
// outside the timed restart path. Filesystem page-cache state is not controlled.
//
// Scenarios:
//   equal: d=0 healthy reboot
//   outside-insert: d new keys above the previous maximum
//   mixed-random: autonomous update/delete/insert divergence on both sides
//
// RBSR and Merkle report warm repair, one-side-cold total, and both-side-cold total.
// RIBLT on-demand is reconstructed every session; CachedEncoder additionally reports warm cached
// repair vs cold cache rebuild+precompute+repair.
//
// Defaults:
//   n = 100_000
//   d = 1_000, 10_000
//
// Overrides:
//   RECONCILE_COLD_N=100000
//   RECONCILE_COLD_D=1000,10000
//   RECONCILE_COLD_SEED=42

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use devkit::protocol_cost::{reconcile, Cost};
use do_riblt::{CachedEncoder, Decoder, Encoder, Peeled, Symbol};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut};
use reconcile::{Entry, FileSnapshot, PersistedState, Persistence, Timestamp};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: &[usize] = &[1_000, 10_000];
const DEFAULT_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const CODED_SYMBOL_BYTES: usize = 24;
const STATE_DIGEST_BYTES: usize = 32;
const MERKLE_FANOUT: usize = 16;
const MERKLE_HASH_BYTES: usize = 32;
const RADIX_DEPTH: usize = 16;
const MERKLE_PREFIX_BYTES: usize = 9;
const SESSION_SEED: u64 = 42;

#[derive(Clone, Debug, Eq, PartialEq)]
struct KvSymbol {
    key: u64,
    digest: u64,
}

impl Symbol<SYMBOL_BYTES> for KvSymbol {
    fn to_bytes(&self) -> [u8; SYMBOL_BYTES] {
        let mut bytes = [0; SYMBOL_BYTES];
        bytes[..8].copy_from_slice(&self.key.to_le_bytes());
        bytes[8..].copy_from_slice(&self.digest.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; SYMBOL_BYTES]) -> Self {
        Self {
            key: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            digest: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
        }
    }
}

struct Corpus {
    name: &'static str,
    left: Vec<(u64, u64)>,
    right: Vec<(u64, u64)>,
    expected: Vec<u64>,
}

struct RadixMerkle {
    rows: BTreeMap<u64, u64>,
    levels: Vec<BTreeMap<u64, [u8; MERKLE_HASH_BYTES]>>,
}

struct LoadedRows {
    rows: Vec<(u64, u64)>,
    file_bytes: u64,
    load: Duration,
    project: Duration,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer"))
    })
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer"))
    })
}

fn divergence_sweep() -> Vec<usize> {
    env::var("RECONCILE_COLD_D").map_or_else(
        |_| DEFAULT_D.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("RECONCILE_COLD_D must be comma-separated"))
                })
                .collect()
        },
    )
}

fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ 0xd6e8_feb8_6659_fd93
}

fn changed_digest(key: u64, variant: u64) -> u64 {
    base_digest(key).wrapping_add(variant.wrapping_mul(2).wrapping_add(1))
}

fn baseline(n: usize) -> Vec<(u64, u64)> {
    (0..n)
        .map(|rank| {
            let key = 2 * rank as u64;
            (key, base_digest(key))
        })
        .collect()
}

fn sampled_ranks(n: usize, d: usize, seed: u64) -> Vec<usize> {
    let mut ranks: Vec<_> = (0..n).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    ranks.shuffle(&mut rng);
    ranks.truncate(d);
    ranks.sort_unstable();
    ranks
}

fn diff_keys(left: &[(u64, u64)], right: &[(u64, u64)]) -> Vec<u64> {
    let left: BTreeMap<_, _> = left.iter().copied().collect();
    let right: BTreeMap<_, _> = right.iter().copied().collect();
    left.keys()
        .chain(right.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect()
}

fn corpus_equal(n: usize) -> Corpus {
    let rows = baseline(n);
    Corpus {
        name: "equal",
        left: rows.clone(),
        right: rows,
        expected: Vec::new(),
    }
}

fn corpus_outside_insert(n: usize, d: usize) -> Corpus {
    let left = baseline(n);
    let mut right = left.clone();
    right.extend((0..d).map(|ordinal| {
        let key = 2 * n as u64 + 1 + 2 * ordinal as u64;
        (key, base_digest(key))
    }));
    let expected = diff_keys(&left, &right);
    Corpus {
        name: "outside-insert",
        left,
        right,
        expected,
    }
}

fn corpus_mixed(n: usize, d: usize, seed: u64) -> Corpus {
    let base = baseline(n);
    let mut left: BTreeMap<_, _> = base.iter().copied().collect();
    let mut right = left.clone();
    let ranks = sampled_ranks(n, d, seed);
    let a = d / 3;
    let b = 2 * d / 3;

    for &rank in &ranks[..a] {
        let key = 2 * rank as u64;
        left.insert(key, changed_digest(key, rank as u64 + 1));
    }
    for &rank in &ranks[a..b] {
        left.remove(&(2 * rank as u64));
    }
    for &rank in &ranks[b..] {
        let key = 2 * rank as u64 + 1;
        right.insert(key, changed_digest(key, rank as u64 + 1));
    }

    let left: Vec<_> = left.into_iter().collect();
    let right: Vec<_> = right.into_iter().collect();
    let expected = diff_keys(&left, &right);
    Corpus {
        name: "mixed-random",
        left,
        right,
        expected,
    }
}

fn save_snapshot(path: &Path, rows: &[(u64, u64)]) {
    let entries: Vec<_> = rows
        .iter()
        .map(|&(key, value)| (key, Entry::present(Timestamp::default(), value)))
        .collect();
    let state = PersistedState::from(entries);
    let backend = FileSnapshot::new(path);
    Persistence::<u64, u64>::save(&backend, &state).expect("save benchmark snapshot");
}

fn load_snapshot_rows(path: &Path) -> LoadedRows {
    let file_bytes = fs::metadata(path).expect("stat benchmark snapshot").len();
    let backend = FileSnapshot::new(path);

    let load_started = Instant::now();
    let state = Persistence::<u64, u64>::load(&backend)
        .expect("load benchmark snapshot")
        .expect("snapshot exists");
    let load = load_started.elapsed();

    let project_started = Instant::now();
    let mut rows: Vec<_> = state
        .entries
        .into_iter()
        .filter_map(|(key, entry)| entry.value().copied().map(|value| (key, value)))
        .collect();
    rows.sort_unstable_by_key(|&(key, _)| key);
    let project = project_started.elapsed();

    LoadedRows {
        rows,
        file_bytes,
        load,
        project,
    }
}

fn build_ftm(rows: &[(u64, u64)]) -> (FingerprintTreeMap<u64, u64>, Duration) {
    let started = Instant::now();
    let mut map = FingerprintTreeMap::new();
    for &(key, digest) in rows {
        map.insert(key, digest);
    }
    (map, started.elapsed())
}

fn rbsr_repair(
    left: &FingerprintTreeMap<u64, u64>,
    right: &FingerprintTreeMap<u64, u64>,
) -> (Cost, Duration, Vec<u64>) {
    let mut candidates = BTreeSet::new();
    let mut price = |key| {
        candidates.insert(key);
        vec![SYMBOL_BYTES]
    };
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let started = Instant::now();
    let cost = reconcile(
        left,
        right,
        &FixedFanOut::new(FanOut::NEGENTROPY),
        Some(&mut price),
        &mut rng,
    );
    let elapsed = started.elapsed();
    let diff = candidates
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect();
    (cost, elapsed, diff)
}

fn leaf_hash(key: u64, digest: u64) -> [u8; MERKLE_HASH_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[RADIX_DEPTH as u8]);
    hasher.update(&key.to_be_bytes());
    hasher.update(&digest.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn parent_hash(depth: usize, children: &[[u8; MERKLE_HASH_BYTES]; MERKLE_FANOUT]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[depth as u8]);
    for child in children {
        hasher.update(child);
    }
    *hasher.finalize().as_bytes()
}

impl RadixMerkle {
    fn build(rows: &[(u64, u64)]) -> (Self, Duration) {
        let started = Instant::now();
        let row_map = rows.iter().copied().collect();
        let mut levels = vec![BTreeMap::new(); RADIX_DEPTH + 1];
        for &(key, digest) in rows {
            levels[RADIX_DEPTH].insert(key, leaf_hash(key, digest));
        }
        for depth in (0..RADIX_DEPTH).rev() {
            let mut grouped: BTreeMap<u64, [[u8; MERKLE_HASH_BYTES]; MERKLE_FANOUT]> =
                BTreeMap::new();
            for (&child, &hash) in &levels[depth + 1] {
                let parent = child >> 4;
                let slot = (child & 0xf) as usize;
                grouped
                    .entry(parent)
                    .or_insert([[0; MERKLE_HASH_BYTES]; MERKLE_FANOUT])[slot] = hash;
            }
            for (parent, children) in grouped {
                levels[depth].insert(parent, parent_hash(depth, &children));
            }
        }
        (
            Self {
                rows: row_map,
                levels,
            },
            started.elapsed(),
        )
    }

    fn hash(&self, depth: usize, prefix: u64) -> [u8; MERKLE_HASH_BYTES] {
        self.levels[depth]
            .get(&prefix)
            .copied()
            .unwrap_or([0; MERKLE_HASH_BYTES])
    }
}

fn merkle_repair(left: &RadixMerkle, right: &RadixMerkle) -> (usize, Duration, Vec<u64>) {
    let started = Instant::now();
    let mut bytes = MERKLE_HASH_BYTES;
    let mut mismatching = if left.hash(0, 0) == right.hash(0, 0) {
        Vec::new()
    } else {
        vec![0u64]
    };

    for depth in 0..RADIX_DEPTH {
        if mismatching.is_empty() {
            break;
        }
        bytes += mismatching.len() * MERKLE_PREFIX_BYTES;
        let mut next = Vec::new();
        for &parent in &mismatching {
            for slot in 0..MERKLE_FANOUT {
                let child = (parent << 4) | slot as u64;
                bytes += MERKLE_HASH_BYTES;
                if left.hash(depth + 1, child) != right.hash(depth + 1, child) {
                    next.push(child);
                }
            }
        }
        mismatching = next;
    }

    if !mismatching.is_empty() {
        bytes += mismatching.len() * 8;
        bytes += mismatching
            .iter()
            .filter(|key| right.rows.contains_key(key))
            .count()
            * SYMBOL_BYTES;
    }
    (bytes, started.elapsed(), mismatching)
}

fn state_digest(rows: &[(u64, u64)]) -> [u8; STATE_DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    for &(key, digest) in rows {
        hasher.update(&key.to_le_bytes());
        hasher.update(&digest.to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn symbols(rows: &[(u64, u64)]) -> impl Iterator<Item = KvSymbol> + '_ {
    rows.iter().map(|&(key, digest)| KvSymbol { key, digest })
}

fn riblt_on_demand(
    left: &[(u64, u64)],
    right: &[(u64, u64)],
) -> (Duration, Duration, usize, Vec<u64>) {
    let setup_started = Instant::now();
    let left_digest = state_digest(left);
    let right_digest = state_digest(right);
    if left_digest == right_digest {
        return (setup_started.elapsed(), Duration::ZERO, 0, Vec::new());
    }
    let mut encoder = Encoder::<SYMBOL_BYTES>::new(symbols(right));
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(symbols(left));
    let setup = setup_started.elapsed();

    let started = Instant::now();
    let mut peeled = Vec::new();
    let mut coded = 0;
    loop {
        let symbol = encoder.next().expect("RIBLT encoder is rateless");
        coded += 1;
        let (done, new_peeled) = decoder.next_symbol(symbol);
        peeled.extend(new_peeled);
        if done {
            break;
        }
    }
    let mut diff: Vec<_> = peeled
        .into_iter()
        .map(|item| match item {
            Peeled::MissingLocal(symbol) | Peeled::MissingRemote(symbol) => symbol.key,
        })
        .collect();
    diff.sort_unstable();
    diff.dedup();
    (setup, started.elapsed(), coded, diff)
}

fn riblt_cached(
    left: &[(u64, u64)],
    right: &[(u64, u64)],
    depth: usize,
) -> (Duration, Duration, Duration, Vec<u64>) {
    if depth == 0 {
        let started = Instant::now();
        let _ = (state_digest(left), state_digest(right));
        return (
            started.elapsed(),
            Duration::ZERO,
            Duration::ZERO,
            Vec::new(),
        );
    }

    let cache_started = Instant::now();
    let mut encoder = CachedEncoder::<SYMBOL_BYTES>::new(symbols(right));
    let build = cache_started.elapsed();

    let precompute_started = Instant::now();
    let _ = encoder.get(depth - 1);
    let precompute = precompute_started.elapsed();

    let session_started = Instant::now();
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(symbols(left));
    let mut peeled = Vec::new();
    for index in 0..depth {
        let (done, new_peeled) = decoder.next_symbol(encoder.get(index));
        peeled.extend(new_peeled);
        if done {
            break;
        }
    }
    let mut diff: Vec<_> = peeled
        .into_iter()
        .map(|item| match item {
            Peeled::MissingLocal(symbol) | Peeled::MissingRemote(symbol) => symbol.key,
        })
        .collect();
    diff.sort_unstable();
    diff.dedup();
    (build, precompute, session_started.elapsed(), diff)
}

fn total_bytes(cost: &Cost) -> usize {
    cost.total_bytes()
        .first()
        .copied()
        .unwrap_or(cost.refinement_bytes)
}

fn ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn run(corpus: Corpus) {
    let (left_ftm, left_ftm_build) = build_ftm(&corpus.left);
    let (right_ftm, right_ftm_build) = build_ftm(&corpus.right);
    let (rbsr, rbsr_warm, rbsr_diff) = rbsr_repair(&left_ftm, &right_ftm);
    assert_eq!(rbsr_diff, corpus.expected);

    let (left_merkle, left_merkle_build) = RadixMerkle::build(&corpus.left);
    let (right_merkle, right_merkle_build) = RadixMerkle::build(&corpus.right);
    let (merkle_bytes, merkle_warm, merkle_diff) = merkle_repair(&left_merkle, &right_merkle);
    assert_eq!(merkle_diff, corpus.expected);

    let (riblt_setup, riblt_repair, coded, riblt_diff) =
        riblt_on_demand(&corpus.left, &corpus.right);
    assert_eq!(riblt_diff, corpus.expected);
    let (cache_build, cache_precompute, cache_warm, cache_diff) =
        riblt_cached(&corpus.left, &corpus.right, coded);
    assert_eq!(cache_diff, corpus.expected);

    println!(
        "[cold-rbsr] scenario={} d={} bytes={} warm={:.3}ms left_build={:.3}ms right_build={:.3}ms one_side_total={:.3}ms both_cold_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        total_bytes(&rbsr),
        ms(rbsr_warm),
        ms(left_ftm_build),
        ms(right_ftm_build),
        ms(right_ftm_build + rbsr_warm),
        ms(left_ftm_build + right_ftm_build + rbsr_warm),
    );
    println!(
        "[cold-merkle] scenario={} d={} bytes={} warm={:.3}ms left_build={:.3}ms right_build={:.3}ms one_side_total={:.3}ms both_cold_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        merkle_bytes,
        ms(merkle_warm),
        ms(left_merkle_build),
        ms(right_merkle_build),
        ms(right_merkle_build + merkle_warm),
        ms(left_merkle_build + right_merkle_build + merkle_warm),
    );
    println!(
        "[cold-riblt] scenario={} d={} bytes={} coded={} ondemand_setup={:.3}ms ondemand_repair={:.3}ms ondemand_total={:.3}ms cache_build={:.3}ms cache_precompute={:.3}ms warm_cached_session={:.3}ms cold_cached_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        STATE_DIGEST_BYTES + coded * CODED_SYMBOL_BYTES,
        coded,
        ms(riblt_setup),
        ms(riblt_repair),
        ms(riblt_setup + riblt_repair),
        ms(cache_build),
        ms(cache_precompute),
        ms(cache_warm),
        ms(cache_build + cache_precompute + cache_warm),
    );

    let dir = tempfile::tempdir().expect("create cold-start benchmark directory");
    let left_path = dir.path().join("left.snapshot");
    let right_path = dir.path().join("right.snapshot");
    save_snapshot(&left_path, &corpus.left);
    save_snapshot(&right_path, &corpus.right);

    let left_loaded = load_snapshot_rows(&left_path);
    let right_loaded = load_snapshot_rows(&right_path);
    assert_eq!(left_loaded.rows, corpus.left);
    assert_eq!(right_loaded.rows, corpus.right);

    let (left_loaded_ftm, left_loaded_ftm_build) = build_ftm(&left_loaded.rows);
    let (right_loaded_ftm, right_loaded_ftm_build) = build_ftm(&right_loaded.rows);
    assert!(left_loaded_ftm.iter().eq(left_ftm.iter()));
    assert!(right_loaded_ftm.iter().eq(right_ftm.iter()));

    let (left_loaded_merkle, left_loaded_merkle_build) = RadixMerkle::build(&left_loaded.rows);
    let (right_loaded_merkle, right_loaded_merkle_build) = RadixMerkle::build(&right_loaded.rows);
    assert_eq!(left_loaded_merkle.hash(0, 0), left_merkle.hash(0, 0));
    assert_eq!(right_loaded_merkle.hash(0, 0), right_merkle.hash(0, 0));

    let left_io = left_loaded.load + left_loaded.project;
    let right_io = right_loaded.load + right_loaded.project;
    let both_io = left_io + right_io;

    println!(
        "[durable-load] scenario={} d={} left_bytes={} right_bytes={} left_load={:.3}ms left_project={:.3}ms right_load={:.3}ms right_project={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        left_loaded.file_bytes,
        right_loaded.file_bytes,
        ms(left_loaded.load),
        ms(left_loaded.project),
        ms(right_loaded.load),
        ms(right_loaded.project),
    );

    println!(
        "[durable-rbsr] scenario={} d={} one_side_ready={:.3}ms both_ready={:.3}ms one_side_total={:.3}ms both_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        ms(right_io + right_loaded_ftm_build),
        ms(both_io + left_loaded_ftm_build + right_loaded_ftm_build),
        ms(right_io + right_loaded_ftm_build + rbsr_warm),
        ms(both_io + left_loaded_ftm_build + right_loaded_ftm_build + rbsr_warm),
    );
    println!(
        "[durable-merkle] scenario={} d={} one_side_ready={:.3}ms both_ready={:.3}ms one_side_total={:.3}ms both_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        ms(right_io + right_loaded_merkle_build),
        ms(both_io + left_loaded_merkle_build + right_loaded_merkle_build),
        ms(right_io + right_loaded_merkle_build + merkle_warm),
        ms(both_io + left_loaded_merkle_build + right_loaded_merkle_build + merkle_warm),
    );
    println!(
        "[durable-riblt] scenario={} d={} one_side_ready={:.3}ms both_ready={:.3}ms one_side_ondemand_total={:.3}ms both_ondemand_total={:.3}ms one_side_cached_total={:.3}ms both_cached_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        ms(right_io),
        ms(both_io),
        ms(right_io + riblt_setup + riblt_repair),
        ms(both_io + riblt_setup + riblt_repair),
        ms(right_io + cache_build + cache_precompute + cache_warm),
        ms(both_io + cache_build + cache_precompute + cache_warm),
    );
}

fn main() {
    let n = env_usize("RECONCILE_COLD_N", DEFAULT_N);
    let seed = env_u64("RECONCILE_COLD_SEED", DEFAULT_SEED);
    assert!(n > 0);

    run(corpus_equal(n));
    for d in divergence_sweep() {
        assert!(d > 0 && d <= n);
        run(corpus_outside_insert(n, d));
        run(corpus_mixed(n, d, seed));
    }
}
