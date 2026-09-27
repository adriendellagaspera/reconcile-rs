// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Filesystem-backed cold-start state-repair benchmark.
//
// This is Phase B of the reboot study. Canonical current rows are first persisted through the real
// FileSnapshot adapter. The timed restart path is then:
//
//   FileSnapshot::load -> materialize current rows -> build reconciliation acceleration -> repair
//
// Both strategies consume the same loaded rows, so filesystem/decode cost is common rather than
// re-read separately for each algorithm. The benchmark reports snapshot bytes, load/decode time,
// row materialization time, acceleration rebuild time, repair time, and total both-sides-cold time.
//
// The GitHub runner's page-cache state is not controllable, so "filesystem-backed" does NOT mean
// physical cold-disk latency. These numbers price fs::read + header validation + bincode decode on
// the runner, not a storage-device guarantee.
//
// Persisted states contain only current live rows. Delete-history/tombstone debt is a separate
// runtime concern already benchmarked by runtime_history_independent_catchup.
//
// Defaults:
//   n = 100_000
//   d = 1_000, 10_000
//
// Overrides:
//   RECONCILE_DURABLE_N=100000
//   RECONCILE_DURABLE_D=1000,10000
//   RECONCILE_DURABLE_SEED=42
//
// Run with `cargo bench --bench durable_cold_start`.

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
use reconcile::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use reconcile::{Entry, FileSnapshot, PersistedState, Persistence};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: &[usize] = &[1_000, 10_000];
const DEFAULT_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const RIBLT_CODED_SYMBOL_BYTES: usize = SYMBOL_BYTES + 8;
const STATE_DIGEST_BYTES: usize = 32;
const MERKLE_FANOUT: usize = 16;
const MERKLE_HASH_BYTES: usize = 32;
const MERKLE_PREFIX_BYTES: usize = 9;
const RADIX_DEPTH: usize = 16;
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

struct LoadedRows {
    rows: Vec<(u64, u64)>,
    file_bytes: u64,
    load: Duration,
    materialize: Duration,
}

struct RadixMerkle {
    rows: BTreeMap<u64, u64>,
    levels: Vec<BTreeMap<u64, [u8; MERKLE_HASH_BYTES]>>,
}

struct RepairReport {
    bytes: usize,
    units: usize,
    repair: Duration,
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
    env::var("RECONCILE_DURABLE_D").map_or_else(
        |_| DEFAULT_D.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("RECONCILE_DURABLE_D must be comma-separated"))
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

fn equal_corpus(n: usize) -> Corpus {
    let rows = baseline(n);
    Corpus {
        name: "equal",
        left: rows.clone(),
        right: rows,
        expected: Vec::new(),
    }
}

fn outside_insert_corpus(n: usize, d: usize) -> Corpus {
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

fn mixed_corpus(n: usize, d: usize, seed: u64) -> Corpus {
    let base = baseline(n);
    let mut left: BTreeMap<_, _> = base.iter().copied().collect();
    let mut right = left.clone();
    let ranks = sampled_ranks(n, d, seed);
    let first = d / 3;
    let second = 2 * d / 3;

    for &rank in &ranks[..first] {
        let key = 2 * rank as u64;
        left.insert(key, changed_digest(key, rank as u64 + 1));
    }
    for &rank in &ranks[first..second] {
        left.remove(&(2 * rank as u64));
    }
    for &rank in &ranks[second..] {
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

fn persisted_state(rows: &[(u64, u64)], node_id: u64) -> PersistedState<u64, u64> {
    let node = NodeId::new(node_id);
    let entries: Vec<_> = rows
        .iter()
        .map(|&(key, digest)| {
            let stamp = Timestamp::new(
                Hlc::new(
                    PhysicalTime::from_millis(key.saturating_add(1)),
                    LogicalCounter::new(0),
                ),
                node,
            );
            (key, Entry::present(stamp, digest))
        })
        .collect();
    PersistedState::from(entries)
}

fn write_snapshot(path: &Path, rows: &[(u64, u64)], node_id: u64) {
    let backend = FileSnapshot::new(path);
    let state = persisted_state(rows, node_id);
    Persistence::<u64, u64>::save(&backend, &state).expect("save benchmark snapshot");
}

fn load_rows(path: &Path) -> LoadedRows {
    let file_bytes = fs::metadata(path).expect("snapshot metadata").len();
    let backend = FileSnapshot::new(path);

    let started = Instant::now();
    let state = Persistence::<u64, u64>::load(&backend)
        .expect("load benchmark snapshot")
        .expect("snapshot must exist");
    let load = started.elapsed();

    let started = Instant::now();
    let rows = state
        .entries
        .into_iter()
        .filter_map(|(key, entry)| entry.value().copied().map(|value| (key, value)))
        .collect();
    let materialize = started.elapsed();

    LoadedRows {
        rows,
        file_bytes,
        load,
        materialize,
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

fn total_bytes(cost: &Cost) -> usize {
    cost.total_bytes()
        .first()
        .copied()
        .unwrap_or(cost.refinement_bytes)
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

fn merkle_repair(left: &RadixMerkle, right: &RadixMerkle) -> (RepairReport, Vec<u64>) {
    let started = Instant::now();
    let mut bytes = MERKLE_HASH_BYTES;
    let mut hashes = 1;
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
                hashes += 1;
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

    (
        RepairReport {
            bytes,
            units: hashes,
            repair: started.elapsed(),
        },
        mismatching,
    )
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
) -> (Duration, RepairReport, Vec<u64>) {
    let setup_started = Instant::now();
    if state_digest(left) == state_digest(right) {
        return (
            setup_started.elapsed(),
            RepairReport {
                bytes: STATE_DIGEST_BYTES,
                units: 0,
                repair: Duration::ZERO,
            },
            Vec::new(),
        );
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

    (
        setup,
        RepairReport {
            bytes: STATE_DIGEST_BYTES + coded * RIBLT_CODED_SYMBOL_BYTES,
            units: coded,
            repair: started.elapsed(),
        },
        diff,
    )
}

fn riblt_cached(
    left: &[(u64, u64)],
    right: &[(u64, u64)],
    depth: usize,
) -> (Duration, Duration, Duration) {
    if depth == 0 {
        let started = Instant::now();
        let _ = (state_digest(left), state_digest(right));
        return (started.elapsed(), Duration::ZERO, Duration::ZERO);
    }

    let started = Instant::now();
    let mut encoder = CachedEncoder::<SYMBOL_BYTES>::new(symbols(right));
    let build = started.elapsed();

    let started = Instant::now();
    let _ = encoder.get(depth - 1);
    let precompute = started.elapsed();

    let started = Instant::now();
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(symbols(left));
    for index in 0..depth {
        let (done, _) = decoder.next_symbol(encoder.get(index));
        if done {
            break;
        }
    }
    (build, precompute, started.elapsed())
}

fn ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn run(corpus: Corpus, dir: &Path) {
    let left_path = dir.join(format!("{}-left.bin", corpus.name));
    let right_path = dir.join(format!("{}-right.bin", corpus.name));
    write_snapshot(&left_path, &corpus.left, 1);
    write_snapshot(&right_path, &corpus.right, 2);

    let left = load_rows(&left_path);
    let right = load_rows(&right_path);
    assert_eq!(diff_keys(&left.rows, &right.rows), corpus.expected);

    let common_load =
        left.load + right.load + left.materialize + right.materialize;

    let (left_ftm, left_ftm_build) = build_ftm(&left.rows);
    let (right_ftm, right_ftm_build) = build_ftm(&right.rows);
    let (rbsr, rbsr_repair_time, rbsr_diff) = rbsr_repair(&left_ftm, &right_ftm);
    assert_eq!(rbsr_diff, corpus.expected);
    let rbsr_total =
        common_load + left_ftm_build + right_ftm_build + rbsr_repair_time;

    let (left_merkle, left_merkle_build) = RadixMerkle::build(&left.rows);
    let (right_merkle, right_merkle_build) = RadixMerkle::build(&right.rows);
    let (merkle, merkle_diff) = merkle_repair(&left_merkle, &right_merkle);
    assert_eq!(merkle_diff, corpus.expected);
    let merkle_total =
        common_load + left_merkle_build + right_merkle_build + merkle.repair;

    let (riblt_setup, riblt, riblt_diff) = riblt_on_demand(&left.rows, &right.rows);
    assert_eq!(riblt_diff, corpus.expected);
    let riblt_total = common_load + riblt_setup + riblt.repair;
    let (cache_build, cache_precompute, cache_session) =
        riblt_cached(&left.rows, &right.rows, riblt.units);
    let riblt_cached_total =
        common_load + cache_build + cache_precompute + cache_session;

    println!(
        "[durable-load] scenario={} d={} left_rows={} right_rows={} left_file={}B right_file={}B left_load={:.3}ms right_load={:.3}ms left_materialize={:.3}ms right_materialize={:.3}ms common_load={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        left.rows.len(),
        right.rows.len(),
        left.file_bytes,
        right.file_bytes,
        ms(left.load),
        ms(right.load),
        ms(left.materialize),
        ms(right.materialize),
        ms(common_load),
    );
    println!(
        "[durable-rbsr] scenario={} d={} bytes={} left_build={:.3}ms right_build={:.3}ms repair={:.3}ms both_cold_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        total_bytes(&rbsr),
        ms(left_ftm_build),
        ms(right_ftm_build),
        ms(rbsr_repair_time),
        ms(rbsr_total),
    );
    println!(
        "[durable-merkle] scenario={} d={} bytes={} hashes={} left_build={:.3}ms right_build={:.3}ms repair={:.3}ms both_cold_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        merkle.bytes,
        merkle.units,
        ms(left_merkle_build),
        ms(right_merkle_build),
        ms(merkle.repair),
        ms(merkle_total),
    );
    println!(
        "[durable-riblt] scenario={} d={} bytes={} coded={} ondemand_setup={:.3}ms repair={:.3}ms both_cold_total={:.3}ms cached_build={:.3}ms cached_precompute={:.3}ms cached_session={:.3}ms cached_both_cold_total={:.3}ms",
        corpus.name,
        corpus.expected.len(),
        riblt.bytes,
        riblt.units,
        ms(riblt_setup),
        ms(riblt.repair),
        ms(riblt_total),
        ms(cache_build),
        ms(cache_precompute),
        ms(cache_session),
        ms(riblt_cached_total),
    );
}

fn main() {
    let n = env_usize("RECONCILE_DURABLE_N", DEFAULT_N);
    let seed = env_u64("RECONCILE_DURABLE_SEED", DEFAULT_SEED);
    assert!(n > 0);

    let dir = tempfile::tempdir().expect("temporary snapshot directory");
    run(equal_corpus(n), dir.path());
    for d in divergence_sweep() {
        assert!(d > 0 && d <= n);
        run(outside_insert_corpus(n, d), dir.path());
        run(mixed_corpus(n, d, seed), dir.path());
    }
}
