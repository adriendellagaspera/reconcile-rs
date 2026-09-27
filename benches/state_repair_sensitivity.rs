// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// State-repair sensitivity benchmark.
//
// This target maps the two Pareto boundaries exposed by #195 without running a full Cartesian
// product:
//
// 1. scale: vary n and d/n at fanout 16 with 16-byte symbols;
// 2. fanout: vary RBSR/Merkle fanout at n=100k with 16-byte symbols;
// 3. symbol: vary 16/32/64-byte symbols at fanout 16.
//
// Workloads:
// - random-update: scattered value divergence;
// - outside-insert: ordered append/outside-range divergence.
//
// RIBLT has no fanout parameter, so fanout mode measures it once per (scenario,d) and then sweeps
// only RBSR/Merkle.
//
// Environment:
//   RECONCILE_SENSITIVITY_MODE=scale|fanout|symbol
//   RECONCILE_SENSITIVITY_N=100000
//   RECONCILE_SENSITIVITY_D=0,100,1000,10000
//   RECONCILE_SENSITIVITY_FANOUTS=4,8,16,32
//   RECONCILE_SENSITIVITY_SYMBOLS=16,32,64
//   RECONCILE_SENSITIVITY_SEED=42

use std::collections::{BTreeMap, BTreeSet};
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
const DEFAULT_D: &[usize] = &[0, 100, 1_000, 10_000];
const DEFAULT_FANOUTS: &[usize] = &[4, 8, 16, 32];
const DEFAULT_SYMBOLS: &[usize] = &[16, 32, 64];
const DEFAULT_SEED: u64 = 42;
const DEFAULT_FANOUT: usize = 16;
const DEFAULT_SYMBOL_BYTES: usize = 16;
const MERKLE_HASH_BYTES: usize = 32;
const MERKLE_PREFIX_BYTES: usize = 9;
const STATE_DIGEST_BYTES: usize = 32;
const SESSION_SEED: u64 = 42;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Scale,
    Fanout,
    Symbol,
}

impl Mode {
    fn parse(raw: &str) -> Self {
        match raw {
            "scale" => Self::Scale,
            "fanout" => Self::Fanout,
            "symbol" => Self::Symbol,
            other => panic!("unknown sensitivity mode: {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    RandomUpdate,
    OutsideInsert,
}

impl Scenario {
    const ALL: [Self; 2] = [Self::RandomUpdate, Self::OutsideInsert];
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RandomUpdate => "random-update",
            Self::OutsideInsert => "outside-insert",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KvSymbol<const N: usize> {
    key: u64,
    digest: u64,
}

impl<const N: usize> Symbol<N> for KvSymbol<N> {
    fn to_bytes(&self) -> [u8; N] {
        assert!(N >= 16);
        let mut bytes = [0; N];
        bytes[..8].copy_from_slice(&self.key.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.digest.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; N]) -> Self {
        assert!(N >= 16);
        Self {
            key: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            digest: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        }
    }
}

struct Corpus {
    left: Vec<(u64, u64)>,
    right: Vec<(u64, u64)>,
    expected: Vec<u64>,
}

struct RadixMerkle {
    rows: BTreeMap<u64, u64>,
    fanout: usize,
    depth: usize,
    levels: Vec<BTreeMap<u64, [u8; MERKLE_HASH_BYTES]>>,
}

struct Report {
    bytes: usize,
    units: usize,
    rounds: usize,
    setup: Duration,
    repair: Duration,
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

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    env::var(name).map_or_else(
        |_| default.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("{name} must be comma-separated integers"))
                })
                .collect()
        },
    )
}

fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ 0xd6e8_feb8_6659_fd93
}

fn changed_digest(key: u64, rank: usize) -> u64 {
    base_digest(key).wrapping_add((rank as u64).wrapping_mul(2).wrapping_add(1))
}

fn baseline(n: usize) -> Vec<(u64, u64)> {
    (0..n)
        .map(|rank| {
            let key = 2 * rank as u64;
            (key, base_digest(key))
        })
        .collect()
}

fn random_ranks(n: usize, d: usize, seed: u64) -> Vec<usize> {
    assert!(d <= n);
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

fn corpus(n: usize, d: usize, scenario: Scenario, seed: u64) -> Corpus {
    assert!(d <= n);
    let left = baseline(n);
    let mut right: BTreeMap<_, _> = left.iter().copied().collect();

    match scenario {
        Scenario::RandomUpdate => {
            for rank in random_ranks(n, d, seed) {
                let key = 2 * rank as u64;
                right.insert(key, changed_digest(key, rank));
            }
        }
        Scenario::OutsideInsert => {
            for ordinal in 0..d {
                let key = 2 * n as u64 + 1 + 2 * ordinal as u64;
                right.insert(key, base_digest(key));
            }
        }
    }

    let right: Vec<_> = right.into_iter().collect();
    let expected = diff_keys(&left, &right);
    assert_eq!(expected.len(), d);
    Corpus {
        left,
        right,
        expected,
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

fn rbsr_report(
    left: &FingerprintTreeMap<u64, u64>,
    right: &FingerprintTreeMap<u64, u64>,
    fanout: usize,
    symbol_bytes: usize,
) -> (Report, Vec<u64>) {
    let mut candidates = BTreeSet::new();
    let mut price = |key| {
        candidates.insert(key);
        vec![symbol_bytes]
    };
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let started = Instant::now();
    let cost = reconcile(
        left,
        right,
        &FixedFanOut::new(FanOut::new(fanout)),
        Some(&mut price),
        &mut rng,
    );
    let repair = started.elapsed();
    let recovered = candidates
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect();
    (
        Report {
            bytes: rbsr_bytes(&cost),
            units: cost.ranges,
            rounds: cost.messages,
            setup: Duration::ZERO,
            repair,
        },
        recovered,
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

fn riblt_generic<const N: usize>(
    left: &[(u64, u64)],
    right: &[(u64, u64)],
) -> (Report, Vec<u64>) {
    let setup_started = Instant::now();
    if state_digest(left) == state_digest(right) {
        return (
            Report {
                bytes: STATE_DIGEST_BYTES,
                units: 0,
                rounds: 1,
                setup: setup_started.elapsed(),
                repair: Duration::ZERO,
            },
            Vec::new(),
        );
    }

    let mut encoder = Encoder::<N>::new(
        right
            .iter()
            .map(|&(key, digest)| KvSymbol::<N> { key, digest }),
    );
    let mut decoder = Decoder::<N, KvSymbol<N>>::new(
        left.iter()
            .map(|&(key, digest)| KvSymbol::<N> { key, digest }),
    );
    let setup = setup_started.elapsed();
    let max_symbols = 1_024 + 2 * left.len().max(right.len());

    let started = Instant::now();
    let mut peeled = Vec::new();
    for index in 0..max_symbols {
        let coded = encoder.next().expect("RIBLT encoder is rateless");
        let (done, new_peeled) = decoder.next_symbol(coded);
        peeled.extend(new_peeled);
        if done {
            let mut recovered: Vec<_> = peeled
                .into_iter()
                .map(|item| match item {
                    Peeled::MissingLocal(symbol) | Peeled::MissingRemote(symbol) => symbol.key,
                })
                .collect();
            recovered.sort_unstable();
            recovered.dedup();
            return (
                Report {
                    bytes: STATE_DIGEST_BYTES + (index + 1) * (N + 8),
                    units: index + 1,
                    rounds: 2,
                    setup,
                    repair: started.elapsed(),
                },
                recovered,
            );
        }
    }
    panic!("RIBLT did not decode within the safety bound");
}

fn riblt_report(
    symbol_bytes: usize,
    left: &[(u64, u64)],
    right: &[(u64, u64)],
) -> (Report, Vec<u64>) {
    match symbol_bytes {
        16 => riblt_generic::<16>(left, right),
        32 => riblt_generic::<32>(left, right),
        64 => riblt_generic::<64>(left, right),
        other => panic!("unsupported RIBLT symbol size: {other}; use 16, 32, or 64"),
    }
}

fn radix_depth(fanout: usize) -> usize {
    assert!(fanout >= 2);
    let target = 1u128 << 64;
    let mut covered = 1u128;
    let mut depth = 0;
    while covered < target {
        covered *= fanout as u128;
        depth += 1;
    }
    depth
}

fn leaf_hash(key: u64, digest: u64) -> [u8; MERKLE_HASH_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&key.to_be_bytes());
    hasher.update(&digest.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn parent_hash(
    depth: usize,
    fanout: usize,
    children: &[[u8; MERKLE_HASH_BYTES]],
) -> [u8; MERKLE_HASH_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(depth as u64).to_le_bytes());
    hasher.update(&(fanout as u64).to_le_bytes());
    for child in children {
        hasher.update(child);
    }
    *hasher.finalize().as_bytes()
}

impl RadixMerkle {
    fn build(rows: &[(u64, u64)], fanout: usize) -> (Self, Duration) {
        let started = Instant::now();
        let depth = radix_depth(fanout);
        let row_map = rows.iter().copied().collect();
        let mut levels = vec![BTreeMap::new(); depth + 1];
        for &(key, digest) in rows {
            levels[depth].insert(key, leaf_hash(key, digest));
        }

        for level in (0..depth).rev() {
            let mut grouped: BTreeMap<u64, Vec<[u8; MERKLE_HASH_BYTES]>> = BTreeMap::new();
            for (&child_prefix, &hash) in &levels[level + 1] {
                let parent = child_prefix / fanout as u64;
                let slot = (child_prefix % fanout as u64) as usize;
                grouped
                    .entry(parent)
                    .or_insert_with(|| vec![[0; MERKLE_HASH_BYTES]; fanout])[slot] = hash;
            }
            for (parent, children) in grouped {
                levels[level].insert(parent, parent_hash(level, fanout, &children));
            }
        }

        (
            Self {
                rows: row_map,
                fanout,
                depth,
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

fn merkle_report(
    left: &RadixMerkle,
    right: &RadixMerkle,
    symbol_bytes: usize,
) -> (Report, Vec<u64>) {
    assert_eq!(left.fanout, right.fanout);
    assert_eq!(left.depth, right.depth);
    let started = Instant::now();
    let mut bytes = MERKLE_HASH_BYTES;
    let mut hashes = 1;
    let mut rounds = 1;
    let mut mismatching = if left.hash(0, 0) == right.hash(0, 0) {
        Vec::new()
    } else {
        vec![0u64]
    };

    for depth in 0..left.depth {
        if mismatching.is_empty() {
            break;
        }
        bytes += mismatching.len() * MERKLE_PREFIX_BYTES;
        let mut next = Vec::new();
        for &parent in &mismatching {
            for slot in 0..left.fanout {
                let Some(child) = parent
                    .checked_mul(left.fanout as u64)
                    .and_then(|value| value.checked_add(slot as u64))
                else {
                    continue;
                };
                bytes += MERKLE_HASH_BYTES;
                hashes += 1;
                if left.hash(depth + 1, child) != right.hash(depth + 1, child) {
                    next.push(child);
                }
            }
        }
        rounds += 1;
        mismatching = next;
    }

    let recovered = mismatching;
    if !recovered.is_empty() {
        bytes += recovered.len() * 8;
        bytes += recovered
            .iter()
            .filter(|key| right.rows.contains_key(key))
            .count()
            * symbol_bytes;
        rounds += 1;
    }

    (
        Report {
            bytes,
            units: hashes,
            rounds,
            setup: Duration::ZERO,
            repair: started.elapsed(),
        },
        recovered,
    )
}

fn rbsr_bytes(cost: &Cost) -> usize {
    cost.total_bytes()
        .first()
        .copied()
        .unwrap_or(cost.refinement_bytes)
}

fn ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn run_case(
    n: usize,
    d: usize,
    scenario: Scenario,
    fanout: usize,
    symbol_bytes: usize,
    include_riblt: bool,
    seed: u64,
) {
    let corpus = corpus(n, d, scenario, seed);

    let (left_ftm, left_ftm_setup) = build_ftm(&corpus.left);
    let (right_ftm, right_ftm_setup) = build_ftm(&corpus.right);
    let (mut rbsr, rbsr_diff) =
        rbsr_report(&left_ftm, &right_ftm, fanout, symbol_bytes);
    rbsr.setup = left_ftm_setup + right_ftm_setup;
    assert_eq!(rbsr_diff, corpus.expected);

    let (left_merkle, left_merkle_setup) = RadixMerkle::build(&corpus.left, fanout);
    let (right_merkle, right_merkle_setup) = RadixMerkle::build(&corpus.right, fanout);
    let (mut merkle, merkle_diff) =
        merkle_report(&left_merkle, &right_merkle, symbol_bytes);
    merkle.setup = left_merkle_setup + right_merkle_setup;
    assert_eq!(merkle_diff, corpus.expected);

    println!(
        "[sensitivity] n={n} d={d} ratio={:.6} scenario={scenario} fanout={fanout} symbol={}B | RBSR bytes={} setup={:.3}ms repair={:.3}ms ranges={} messages={} | Merkle bytes={} setup={:.3}ms repair={:.3}ms hashes={} rounds={}",
        if n == 0 { 0.0 } else { d as f64 / n as f64 },
        symbol_bytes,
        rbsr.bytes,
        ms(rbsr.setup),
        ms(rbsr.repair),
        rbsr.units,
        rbsr.rounds,
        merkle.bytes,
        ms(merkle.setup),
        ms(merkle.repair),
        merkle.units,
        merkle.rounds,
    );

    if include_riblt {
        let (riblt, riblt_diff) = riblt_report(symbol_bytes, &corpus.left, &corpus.right);
        assert_eq!(riblt_diff, corpus.expected);
        println!(
            "[sensitivity-riblt] n={n} d={d} ratio={:.6} scenario={scenario} symbol={}B | bytes={} setup={:.3}ms repair={:.3}ms coded={}",
            if n == 0 { 0.0 } else { d as f64 / n as f64 },
            symbol_bytes,
            riblt.bytes,
            ms(riblt.setup),
            ms(riblt.repair),
            riblt.units,
        );
    }
}

fn main() {
    let mode = Mode::parse(
        &env::var("RECONCILE_SENSITIVITY_MODE").unwrap_or_else(|_| "scale".to_string()),
    );
    let n = env_usize("RECONCILE_SENSITIVITY_N", DEFAULT_N);
    let d_values = env_list("RECONCILE_SENSITIVITY_D", DEFAULT_D);
    let fanouts = env_list("RECONCILE_SENSITIVITY_FANOUTS", DEFAULT_FANOUTS);
    let symbols = env_list("RECONCILE_SENSITIVITY_SYMBOLS", DEFAULT_SYMBOLS);
    let seed = env_u64("RECONCILE_SENSITIVITY_SEED", DEFAULT_SEED);

    assert!(n > 0);
    assert!(d_values.iter().all(|&d| d <= n));
    assert!(fanouts.iter().all(|&fanout| fanout >= 2));
    assert!(symbols.iter().all(|size| [16, 32, 64].contains(size)));

    match mode {
        Mode::Scale => {
            for d in d_values {
                for scenario in Scenario::ALL {
                    run_case(
                        n,
                        d,
                        scenario,
                        DEFAULT_FANOUT,
                        DEFAULT_SYMBOL_BYTES,
                        true,
                        seed,
                    );
                }
            }
        }
        Mode::Fanout => {
            for d in d_values {
                for scenario in Scenario::ALL {
                    for (index, &fanout) in fanouts.iter().enumerate() {
                        run_case(
                            n,
                            d,
                            scenario,
                            fanout,
                            DEFAULT_SYMBOL_BYTES,
                            index == 0,
                            seed,
                        );
                    }
                }
            }
        }
        Mode::Symbol => {
            for d in d_values {
                for scenario in Scenario::ALL {
                    for &symbol_bytes in &symbols {
                        run_case(
                            n,
                            d,
                            scenario,
                            DEFAULT_FANOUT,
                            symbol_bytes,
                            true,
                            seed,
                        );
                    }
                }
            }
        }
    }
}
