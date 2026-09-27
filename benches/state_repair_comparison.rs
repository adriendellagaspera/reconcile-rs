// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Prepared-state repair comparison: RBSR vs Rateless IBLT vs Merkle.
//
// Every strategy sees the same sorted key-value manifest. Both replicas contain the same n keys;
// exactly d values have a different 64-bit digest. A changed row is two set symbols for RIBLT:
// (key, old_digest) exists only on the left and (key, new_digest) only on the right.
//
// The benchmark measures divergence discovery metadata only. Application payload transfer is
// excluded. Index/sketch preparation is excluded from the repair timer and reported separately.
// Divergence placement is varied independently from d so ordered locality can be measured.
//
// Defaults:
//   n = 100_000 rows
//   d = 0, 1, 10, 100, 1_000, 10_000 divergent keys
//   profiles = contiguous, clustered-4, clustered-16, uniform-random, evenly-spaced, max-spread
//   random seeds = 3
//   Merkle fanout = 16
//
// Overrides:
//   RECONCILE_STATE_REPAIR_N=1000000
//   RECONCILE_STATE_REPAIR_D=0,10,1000
//   RECONCILE_STATE_REPAIR_PROFILES=contiguous,uniform-random,max-spread
//   RECONCILE_STATE_REPAIR_RANDOM_SEEDS=20
//   RECONCILE_STATE_REPAIR_SEED_BASE=42
//
// Run with `cargo bench --bench state_repair_comparison`.

use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::time::{Duration, Instant};

use devkit::protocol_cost::{reconcile, Cost};
use do_riblt::{Decoder, Encoder, Peeled, Symbol};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: &[usize] = &[0, 1, 10, 100, 1_000, 10_000];
const DEFAULT_RANDOM_SEEDS: usize = 3;
const DEFAULT_SEED_BASE: u64 = 42;
const MERKLE_FANOUT: usize = 16;
const SESSION_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const STATE_DIGEST_BYTES: usize = 32;
const RIBLT_CODED_SYMBOL_BYTES: usize = SYMBOL_BYTES + 8;
const MERKLE_HASH_BYTES: usize = 32;
const MERKLE_INDEX_BYTES: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Profile {
    Contiguous,
    Clustered4,
    Clustered16,
    UniformRandom,
    EvenlySpaced,
    MaxSpread,
}

impl Profile {
    const ALL: [Self; 6] = [
        Self::Contiguous,
        Self::Clustered4,
        Self::Clustered16,
        Self::UniformRandom,
        Self::EvenlySpaced,
        Self::MaxSpread,
    ];

    fn parse(raw: &str) -> Self {
        match raw.trim() {
            "contiguous" => Self::Contiguous,
            "clustered-4" => Self::Clustered4,
            "clustered-16" => Self::Clustered16,
            "uniform-random" => Self::UniformRandom,
            "evenly-spaced" => Self::EvenlySpaced,
            "max-spread" => Self::MaxSpread,
            other => panic!("unknown state-repair profile: {other}"),
        }
    }

    fn is_random(self) -> bool {
        self == Self::UniformRandom
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Contiguous => "contiguous",
            Self::Clustered4 => "clustered-4",
            Self::Clustered16 => "clustered-16",
            Self::UniformRandom => "uniform-random",
            Self::EvenlySpaced => "evenly-spaced",
            Self::MaxSpread => "max-spread",
        })
    }
}

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
    expected_diff: Vec<u64>,
    left_rows: Vec<(u64, u64)>,
    right_rows: Vec<(u64, u64)>,
}

struct MerkleTree {
    rows: Vec<(u64, u64)>,
    // level 0 is one hash per row; the final level contains one root hash.
    levels: Vec<Vec<[u8; MERKLE_HASH_BYTES]>>,
}

struct Report {
    bytes: usize,
    units: usize,
    messages: usize,
    rounds: usize,
    elapsed: Duration,
}

struct RibltReport {
    setup: Duration,
    report: Report,
    recovered_keys: Vec<u64>,
}

struct MerkleReport {
    report: Report,
    hashes_sent: usize,
    recovered_keys: Vec<u64>,
}

struct CaseResult {
    rbsr_setup: Duration,
    rbsr: Cost,
    rbsr_elapsed: Duration,
    riblt: RibltReport,
    merkle_setup: Duration,
    merkle: MerkleReport,
}

#[derive(Debug)]
struct Summary {
    min: usize,
    p50: usize,
    mean: f64,
    p90: usize,
    max: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
    })
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
    })
}

fn divergence_sweep() -> Vec<usize> {
    env::var("RECONCILE_STATE_REPAIR_D").map_or_else(
        |_| DEFAULT_D.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!(
                            "RECONCILE_STATE_REPAIR_D must be a comma-separated list of integers"
                        )
                    })
                })
                .collect()
        },
    )
}

fn profiles() -> Vec<Profile> {
    env::var("RECONCILE_STATE_REPAIR_PROFILES").map_or_else(
        |_| Profile::ALL.to_vec(),
        |raw| raw.split(',').map(Profile::parse).collect(),
    )
}

fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ 0xd6e8_feb8_6659_fd93
}

fn changed_digest(key: u64) -> u64 {
    base_digest(key) ^ 0xa5a5_5a5a_d3c3_b4b4
}

fn evenly_spaced_keys(n: usize, d: usize) -> Vec<u64> {
    (0..d)
        .map(|i| (((2 * i + 1) * n) / (2 * d)) as u64)
        .collect()
}

fn contiguous_keys(n: usize, d: usize) -> Vec<u64> {
    let start = (n - d) / 2;
    (start..start + d).map(|key| key as u64).collect()
}

fn clustered_keys(n: usize, d: usize, clusters: usize) -> Vec<u64> {
    let active_clusters = clusters.min(d).min(n);
    if active_clusters == 0 {
        return Vec::new();
    }

    let mut keys = Vec::with_capacity(d);
    let mut remaining = d;
    for cluster in 0..active_clusters {
        let slots_left = active_clusters - cluster;
        let count = remaining.div_ceil(slots_left);
        let partition_start = cluster * n / active_clusters;
        let partition_end = (cluster + 1) * n / active_clusters;
        let partition_len = partition_end - partition_start;
        assert!(count <= partition_len, "cluster does not fit its key partition");
        let start = partition_start + (partition_len - count) / 2;
        keys.extend((start..start + count).map(|key| key as u64));
        remaining -= count;
    }
    keys.sort_unstable();
    keys
}

fn random_keys(n: usize, d: usize, seed: u64) -> Vec<u64> {
    let mut ranks: Vec<_> = (0..n).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    ranks.shuffle(&mut rng);
    ranks.truncate(d);
    ranks.sort_unstable();
    ranks.into_iter().map(|rank| rank as u64).collect()
}

fn reversed_base16(mut value: usize, digits: usize) -> usize {
    let mut reversed = 0usize;
    for _ in 0..digits {
        reversed = reversed * MERKLE_FANOUT + value % MERKLE_FANOUT;
        value /= MERKLE_FANOUT;
    }
    reversed
}

fn max_spread_keys(n: usize, d: usize) -> Vec<u64> {
    if d == 0 {
        return Vec::new();
    }

    let mut digits = 1;
    let mut space = MERKLE_FANOUT;
    while space < n {
        space *= MERKLE_FANOUT;
        digits += 1;
    }

    let mut keys = Vec::with_capacity(d);
    for ordinal in 0..space {
        let rank = reversed_base16(ordinal, digits);
        if rank < n {
            keys.push(rank as u64);
            if keys.len() == d {
                break;
            }
        }
    }
    assert_eq!(keys.len(), d, "max-spread permutation did not cover d keys");
    keys.sort_unstable();
    keys
}

fn divergent_keys(n: usize, d: usize, profile: Profile, seed: u64) -> Vec<u64> {
    assert!(d <= n, "d must not exceed n");
    if d == 0 {
        return Vec::new();
    }

    let keys = match profile {
        Profile::Contiguous => contiguous_keys(n, d),
        Profile::Clustered4 => clustered_keys(n, d, 4),
        Profile::Clustered16 => clustered_keys(n, d, 16),
        Profile::UniformRandom => random_keys(n, d, seed),
        Profile::EvenlySpaced => evenly_spaced_keys(n, d),
        Profile::MaxSpread => max_spread_keys(n, d),
    };
    assert_eq!(keys.len(), d);
    assert!(keys.windows(2).all(|window| window[0] < window[1]));
    keys
}

fn corpus(n: usize, d: usize, profile: Profile, seed: u64) -> Corpus {
    let expected_diff = divergent_keys(n, d, profile, seed);
    let changed: BTreeSet<_> = expected_diff.iter().copied().collect();
    let left_rows: Vec<_> = (0..n as u64).map(|key| (key, base_digest(key))).collect();
    let right_rows = left_rows
        .iter()
        .map(|&(key, digest)| {
            if changed.contains(&key) {
                (key, changed_digest(key))
            } else {
                (key, digest)
            }
        })
        .collect();
    Corpus {
        expected_diff,
        left_rows,
        right_rows,
    }
}

fn fingerprint_map(rows: &[(u64, u64)]) -> FingerprintTreeMap<u64, u64> {
    let mut map = FingerprintTreeMap::new();
    for &(key, digest) in rows {
        map.insert(key, digest);
    }
    map
}

fn symbols(rows: &[(u64, u64)]) -> Vec<KvSymbol> {
    rows.iter()
        .map(|&(key, digest)| KvSymbol { key, digest })
        .collect()
}

fn state_digest(rows: &[(u64, u64)]) -> [u8; STATE_DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    for &(key, digest) in rows {
        hasher.update(&key.to_le_bytes());
        hasher.update(&digest.to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn rbsr_report(
    left: &FingerprintTreeMap<u64, u64>,
    right: &FingerprintTreeMap<u64, u64>,
) -> (Cost, Duration, Vec<u64>) {
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let mut candidates = BTreeSet::new();
    let mut price = |key| {
        candidates.insert(key);
        vec![SYMBOL_BYTES]
    };
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let started = Instant::now();
    let cost = reconcile(left, right, &policy, Some(&mut price), &mut rng);
    let elapsed = started.elapsed();
    let recovered = candidates
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect();
    (cost, elapsed, recovered)
}

fn riblt_report(left: &[(u64, u64)], right: &[(u64, u64)]) -> RibltReport {
    let setup_started = Instant::now();
    let left_digest = state_digest(left);
    let right_digest = state_digest(right);
    if left_digest == right_digest {
        return RibltReport {
            setup: setup_started.elapsed(),
            report: Report {
                bytes: STATE_DIGEST_BYTES,
                units: 0,
                messages: 1,
                rounds: 1,
                elapsed: Duration::ZERO,
            },
            recovered_keys: Vec::new(),
        };
    }

    let left = symbols(left);
    let right = symbols(right);
    let symbol_count = left.len().max(right.len());
    let mut encoder = Encoder::<SYMBOL_BYTES>::new(right.into_iter());
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(left.into_iter());
    let setup = setup_started.elapsed();
    let max_symbols = 1_024 + 2 * symbol_count;

    let started = Instant::now();
    let mut recovered = Vec::new();
    for index in 0..max_symbols {
        let coded = encoder.next().expect("RIBLT encoder is rateless");
        let (done, peeled) = decoder.next_symbol(coded);
        recovered.extend(peeled);
        if done {
            let mut recovered_keys: Vec<_> = recovered
                .into_iter()
                .map(|item| match item {
                    Peeled::MissingLocal(symbol) | Peeled::MissingRemote(symbol) => symbol.key,
                })
                .collect();
            recovered_keys.sort_unstable();
            recovered_keys.dedup();
            return RibltReport {
                setup,
                report: Report {
                    bytes: STATE_DIGEST_BYTES + (index + 1) * RIBLT_CODED_SYMBOL_BYTES,
                    units: index + 1,
                    messages: 2,
                    rounds: 2,
                    elapsed: started.elapsed(),
                },
                recovered_keys,
            };
        }
    }
    panic!("RIBLT did not decode within the safety bound");
}

fn leaf_hash(key: u64, digest: u64) -> [u8; MERKLE_HASH_BYTES] {
    let mut input = [0u8; SYMBOL_BYTES];
    input[..8].copy_from_slice(&key.to_le_bytes());
    input[8..].copy_from_slice(&digest.to_le_bytes());
    *blake3::hash(&input).as_bytes()
}

fn parent_hash(children: &[[u8; MERKLE_HASH_BYTES]]) -> [u8; MERKLE_HASH_BYTES] {
    let mut hasher = blake3::Hasher::new();
    for child in children {
        hasher.update(child);
    }
    *hasher.finalize().as_bytes()
}

impl MerkleTree {
    fn new(rows: &[(u64, u64)]) -> Self {
        assert!(!rows.is_empty(), "Merkle corpus must not be empty");
        let mut levels = vec![rows
            .iter()
            .map(|&(key, digest)| leaf_hash(key, digest))
            .collect::<Vec<_>>()];
        while levels.last().unwrap().len() > 1 {
            let parent = levels
                .last()
                .unwrap()
                .chunks(MERKLE_FANOUT)
                .map(parent_hash)
                .collect();
            levels.push(parent);
        }
        Self {
            rows: rows.to_vec(),
            levels,
        }
    }

    fn root(&self) -> [u8; MERKLE_HASH_BYTES] {
        self.levels.last().unwrap()[0]
    }
}

fn merkle_report(left: &MerkleTree, right: &MerkleTree) -> MerkleReport {
    assert_eq!(left.rows.len(), right.rows.len());
    assert_eq!(left.levels.len(), right.levels.len());

    let started = Instant::now();
    let mut bytes = MERKLE_HASH_BYTES;
    let mut hashes_sent = 1;
    let mut messages = 1;
    let mut rounds = 1;
    let top = left.levels.len() - 1;
    let mut mismatching = if left.root() == right.root() {
        Vec::new()
    } else {
        vec![0usize]
    };

    for level in (1..=top).rev() {
        if mismatching.is_empty() {
            break;
        }

        bytes += mismatching.len() * MERKLE_INDEX_BYTES;
        messages += 1;
        let child_level = level - 1;
        let mut next = Vec::new();
        for parent in &mismatching {
            let start = parent * MERKLE_FANOUT;
            let end = (start + MERKLE_FANOUT).min(right.levels[child_level].len());
            for child in start..end {
                bytes += MERKLE_HASH_BYTES;
                hashes_sent += 1;
                if left.levels[child_level][child] != right.levels[child_level][child] {
                    next.push(child);
                }
            }
        }
        messages += 1;
        rounds += 1;
        mismatching = next;
    }

    let mut recovered_keys = Vec::with_capacity(mismatching.len());
    if !mismatching.is_empty() {
        bytes += mismatching.len() * MERKLE_INDEX_BYTES;
        messages += 1;
        bytes += mismatching.len() * SYMBOL_BYTES;
        messages += 1;
        rounds += 1;
        for &index in &mismatching {
            assert_eq!(left.rows[index].0, right.rows[index].0);
            recovered_keys.push(left.rows[index].0);
        }
    }

    MerkleReport {
        report: Report {
            bytes,
            units: hashes_sent,
            messages,
            rounds,
            elapsed: started.elapsed(),
        },
        hashes_sent,
        recovered_keys,
    }
}

fn run_case(n: usize, d: usize, profile: Profile, seed: u64) -> CaseResult {
    let corpus = corpus(n, d, profile, seed);

    let rbsr_setup_started = Instant::now();
    let left_map = fingerprint_map(&corpus.left_rows);
    let right_map = fingerprint_map(&corpus.right_rows);
    let rbsr_setup = rbsr_setup_started.elapsed();
    let (rbsr, rbsr_elapsed, rbsr_recovered) = rbsr_report(&left_map, &right_map);
    assert_eq!(
        rbsr_recovered, corpus.expected_diff,
        "RBSR recovered a different business-key set"
    );

    let riblt = riblt_report(&corpus.left_rows, &corpus.right_rows);
    assert_eq!(
        riblt.recovered_keys, corpus.expected_diff,
        "RIBLT recovered a different business-key set"
    );

    let merkle_setup_started = Instant::now();
    let left_merkle = MerkleTree::new(&corpus.left_rows);
    let right_merkle = MerkleTree::new(&corpus.right_rows);
    let merkle_setup = merkle_setup_started.elapsed();
    let merkle = merkle_report(&left_merkle, &right_merkle);
    assert_eq!(
        merkle.recovered_keys, corpus.expected_diff,
        "Merkle recovered a different business-key set"
    );

    CaseResult {
        rbsr_setup,
        rbsr,
        rbsr_elapsed,
        riblt,
        merkle_setup,
        merkle,
    }
}

fn rbsr_bytes(cost: &Cost) -> usize {
    cost.total_bytes()
        .first()
        .copied()
        .unwrap_or(cost.refinement_bytes)
}

fn summarize(values: &[usize]) -> Summary {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let percentile = |p: usize| {
        let rank = (p * sorted.len()).div_ceil(100).max(1);
        sorted[rank - 1]
    };
    Summary {
        min: sorted[0],
        p50: percentile(50),
        mean: sorted.iter().sum::<usize>() as f64 / sorted.len() as f64,
        p90: percentile(90),
        max: *sorted.last().unwrap(),
    }
}

fn print_case(n: usize, d: usize, profile: Profile, seed: u64, case: &CaseResult) {
    println!(
        "[state-repair-case] n={n} d={d} profile={profile} seed={seed} symbol={SYMBOL_BYTES}B fanout={MERKLE_FANOUT} | RBSR bytes={} ranges={} idlist={} time={:.3}ms | RIBLT bytes={} coded={} time={:.3}ms | Merkle bytes={} hashes={} rounds={} time={:.3}ms",
        rbsr_bytes(&case.rbsr),
        case.rbsr.ranges,
        case.rbsr.enumerated_elements,
        case.rbsr_elapsed.as_secs_f64() * 1_000.0,
        case.riblt.report.bytes,
        case.riblt.report.units,
        case.riblt.report.elapsed.as_secs_f64() * 1_000.0,
        case.merkle.report.bytes,
        case.merkle.hashes_sent,
        case.merkle.report.rounds,
        case.merkle.report.elapsed.as_secs_f64() * 1_000.0,
    );
    println!(
        "[state-repair-setup] n={n} d={d} profile={profile} seed={seed} | RBSR={:.3}ms RIBLT={:.3}ms Merkle={:.3}ms",
        case.rbsr_setup.as_secs_f64() * 1_000.0,
        case.riblt.setup.as_secs_f64() * 1_000.0,
        case.merkle_setup.as_secs_f64() * 1_000.0,
    );
}

fn print_random_summary(n: usize, d: usize, seeds: &[u64], cases: &[CaseResult]) {
    let rbsr = summarize(&cases.iter().map(|case| rbsr_bytes(&case.rbsr)).collect::<Vec<_>>());
    let riblt = summarize(
        &cases
            .iter()
            .map(|case| case.riblt.report.bytes)
            .collect::<Vec<_>>(),
    );
    let merkle = summarize(
        &cases
            .iter()
            .map(|case| case.merkle.report.bytes)
            .collect::<Vec<_>>(),
    );
    println!(
        "[state-repair-summary] n={n} d={d} profile=uniform-random samples={} seed={}..{} | RBSR bytes min={} p50={} mean={:.1} p90={} max={} | RIBLT bytes min={} p50={} mean={:.1} p90={} max={} | Merkle bytes min={} p50={} mean={:.1} p90={} max={}",
        cases.len(),
        seeds.first().unwrap(),
        seeds.last().unwrap(),
        rbsr.min,
        rbsr.p50,
        rbsr.mean,
        rbsr.p90,
        rbsr.max,
        riblt.min,
        riblt.p50,
        riblt.mean,
        riblt.p90,
        riblt.max,
        merkle.min,
        merkle.p50,
        merkle.mean,
        merkle.p90,
        merkle.max,
    );
}

fn main() {
    let n = env_usize("RECONCILE_STATE_REPAIR_N", DEFAULT_N);
    let sweep = divergence_sweep();
    let profiles = profiles();
    let random_seeds = env_usize(
        "RECONCILE_STATE_REPAIR_RANDOM_SEEDS",
        DEFAULT_RANDOM_SEEDS,
    );
    let seed_base = env_u64("RECONCILE_STATE_REPAIR_SEED_BASE", DEFAULT_SEED_BASE);
    assert!(!sweep.is_empty(), "at least one d is required");
    assert!(!profiles.is_empty(), "at least one profile is required");
    assert!(
        !profiles.iter().any(|profile| profile.is_random()) || random_seeds > 0,
        "uniform-random requires at least one seed"
    );

    println!(
        "[state-repair] n={n}; update-only aligned key universe; payload transfer excluded; RIBLT includes a {STATE_DIGEST_BYTES} B equality preflight"
    );
    println!(
        "[state-repair] RIBLT coded symbol={RIBLT_CODED_SYMBOL_BYTES} B; RBSR enumerated row={SYMBOL_BYTES} B; Merkle hash={MERKLE_HASH_BYTES} B; fanout={MERKLE_FANOUT}"
    );

    for d in sweep {
        assert!(d <= n, "d must not exceed n");
        for profile in &profiles {
            if profile.is_random() {
                let seeds: Vec<_> = (0..random_seeds)
                    .map(|offset| seed_base.wrapping_add(offset as u64))
                    .collect();
                let mut cases = Vec::with_capacity(seeds.len());
                for &seed in &seeds {
                    let case = run_case(n, d, *profile, seed);
                    print_case(n, d, *profile, seed, &case);
                    cases.push(case);
                }
                print_random_summary(n, d, &seeds, &cases);
            } else {
                let case = run_case(n, d, *profile, seed_base);
                print_case(n, d, *profile, seed_base, &case);
            }
        }
    }
}
