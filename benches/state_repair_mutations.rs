// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Arbitrary-keyspace state-repair comparison.
//
// Unlike state_repair_comparison, this benchmark allows the replicas' key sets and cardinalities to
// differ. Baseline keys are even u64s, leaving odd in-range keys available for true interleaved
// insertions. The Merkle baseline is a fixed 16-way sparse radix tree over key bits, so insertion
// or deletion changes only the key's root-to-leaf path rather than shifting positional leaves.
//
// Payload transfer is excluded. All three strategies must recover the same business-key diff.
//
// Defaults:
//   n = 100_000 baseline rows
//   d = 100, 1_000, 10_000 business keys changed per scenario
//
// Overrides:
//   RECONCILE_MUTATION_N=100000
//   RECONCILE_MUTATION_D=100,1000
//   RECONCILE_MUTATION_SEED=42
//
// Run with `cargo bench --bench state_repair_mutations`.

use devkit::experiment::producer::{
    elapsed, protocol_bytes, write_case_from_env, write_repair_trace_from_env, Arm, Case,
};
use devkit::experiment::{CostOwner, LifecyclePhase, RepairStage, RepairStrategy, RepairTrace};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::time::{Duration, Instant};

use devkit::corpus::mutation::{corpus, Corpus, Scenario};
use devkit::protocol_cost::{reconcile_traced, Cost};
use do_riblt::{Decoder, Encoder, Peeled, Symbol};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: &[usize] = &[100, 1_000, 10_000];
const DEFAULT_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const RIBLT_CODED_SYMBOL_BYTES: usize = SYMBOL_BYTES + 8;
const STATE_DIGEST_BYTES: usize = 32;
const MERKLE_FANOUT: usize = 16;
const MERKLE_HASH_BYTES: usize = 32;
const MERKLE_PREFIX_BYTES: usize = 9; // depth + u64 prefix
const RADIX_DEPTH: usize = 16;
const SESSION_SEED: u64 = 42;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
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

struct RadixMerkle {
    rows: BTreeMap<u64, u64>,
    // levels[depth][prefix], depth 0=root, depth 16=full u64 key.
    levels: Vec<BTreeMap<u64, [u8; MERKLE_HASH_BYTES]>>,
}

struct Report {
    bytes: usize,
    units: usize,
    rounds: usize,
    elapsed: Duration,
    trace: RepairTrace,
}

struct CaseResult {
    rbsr_setup: Duration,
    rbsr: Cost,
    rbsr_time: Duration,
    riblt_setup: Duration,
    riblt: Report,
    merkle_setup: Duration,
    merkle: Report,
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
    env::var("RECONCILE_MUTATION_D").map_or_else(
        |_| DEFAULT_D.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!("RECONCILE_MUTATION_D must be comma-separated integers")
                    })
                })
                .collect()
        },
    )
}

fn fingerprint_map(rows: &[(u64, u64)]) -> FingerprintTreeMap<u64, u64> {
    let mut map = FingerprintTreeMap::new();
    for &(key, digest) in rows {
        map.insert(key, digest);
    }
    map
}

fn rbsr_report(
    left: &FingerprintTreeMap<u64, u64>,
    right: &FingerprintTreeMap<u64, u64>,
) -> (Cost, Duration, Vec<u64>, RepairTrace) {
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let mut candidates = BTreeSet::new();
    let mut price = |key| {
        candidates.insert(key);
        vec![SYMBOL_BYTES]
    };
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let started = Instant::now();
    let (cost, trace) = reconcile_traced(left, right, &policy, Some(&mut price), &mut rng);
    let elapsed = started.elapsed();
    let recovered = candidates
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect();
    assert_eq!(trace.total_byte_variants(), vec![rbsr_bytes(&cost) as u64]);
    (cost, elapsed, recovered, trace)
}

fn state_digest(rows: &[(u64, u64)]) -> [u8; STATE_DIGEST_BYTES] {
    let mut hasher = blake3::Hasher::new();
    for &(key, digest) in rows {
        hasher.update(&key.to_le_bytes());
        hasher.update(&digest.to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn riblt_report(left: &[(u64, u64)], right: &[(u64, u64)]) -> (Duration, Report, Vec<u64>) {
    let setup_started = Instant::now();
    if state_digest(left) == state_digest(right) {
        return (
            setup_started.elapsed(),
            Report {
                bytes: STATE_DIGEST_BYTES,
                units: 0,
                rounds: 1,
                elapsed: Duration::ZERO,
                trace: RepairTrace::new(
                    RepairStrategy::Riblt,
                    vec![RepairStage::RibltEquality {
                        bytes: STATE_DIGEST_BYTES as u64,
                    }],
                ),
            },
            Vec::new(),
        );
    }

    let mut encoder =
        Encoder::<SYMBOL_BYTES>::new(right.iter().map(|&(key, digest)| KvSymbol { key, digest }));
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(
        left.iter().map(|&(key, digest)| KvSymbol { key, digest }),
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
                setup,
                Report {
                    bytes: STATE_DIGEST_BYTES + (index + 1) * RIBLT_CODED_SYMBOL_BYTES,
                    units: index + 1,
                    rounds: 2,
                    elapsed: started.elapsed(),
                    trace: RepairTrace::new(
                        RepairStrategy::Riblt,
                        vec![
                            RepairStage::RibltEquality {
                                bytes: STATE_DIGEST_BYTES as u64,
                            },
                            RepairStage::RibltStream {
                                coded_symbols: (index + 1) as u64,
                                coded_symbol_bytes: RIBLT_CODED_SYMBOL_BYTES as u64,
                            },
                            RepairStage::RibltStopAck { bytes: 0 },
                        ],
                    ),
                },
                recovered,
            );
        }
    }
    panic!("RIBLT did not decode within the safety bound");
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
    fn new(rows: &[(u64, u64)]) -> Self {
        let row_map: BTreeMap<_, _> = rows.iter().copied().collect();
        let mut levels = vec![BTreeMap::new(); RADIX_DEPTH + 1];

        for &(key, digest) in rows {
            levels[RADIX_DEPTH].insert(key, leaf_hash(key, digest));
        }

        for depth in (0..RADIX_DEPTH).rev() {
            let mut grouped: BTreeMap<u64, [[u8; MERKLE_HASH_BYTES]; MERKLE_FANOUT]> =
                BTreeMap::new();
            for (&child_prefix, &hash) in &levels[depth + 1] {
                let parent = child_prefix >> 4;
                let slot = (child_prefix & 0xf) as usize;
                grouped
                    .entry(parent)
                    .or_insert([[0; MERKLE_HASH_BYTES]; MERKLE_FANOUT])[slot] = hash;
            }
            for (parent, children) in grouped {
                levels[depth].insert(parent, parent_hash(depth, &children));
            }
        }

        Self {
            rows: row_map,
            levels,
        }
    }

    fn hash(&self, depth: usize, prefix: u64) -> [u8; MERKLE_HASH_BYTES] {
        self.levels[depth]
            .get(&prefix)
            .copied()
            .unwrap_or([0; MERKLE_HASH_BYTES])
    }
}

fn merkle_report(left: &RadixMerkle, right: &RadixMerkle) -> (Report, Vec<u64>) {
    let started = Instant::now();
    let mut bytes = MERKLE_HASH_BYTES;
    let mut hashes = 1;
    let mut rounds = 1;
    let mut stages = vec![RepairStage::MerkleExchange {
        depth: 0,
        request_prefixes: 0,
        request_bytes: 0,
        response_hashes: 1,
        response_bytes: MERKLE_HASH_BYTES as u64,
    }];
    let mut mismatching = if left.hash(0, 0) == right.hash(0, 0) {
        Vec::new()
    } else {
        vec![0u64]
    };

    for depth in 0..RADIX_DEPTH {
        if mismatching.is_empty() {
            break;
        }

        let request_prefixes = mismatching.len();
        let request_bytes = request_prefixes * MERKLE_PREFIX_BYTES;
        bytes += request_bytes;
        let mut next = Vec::new();
        let mut response_hashes = 0usize;
        for &parent in &mismatching {
            for slot in 0..MERKLE_FANOUT {
                let child = (parent << 4) | slot as u64;
                bytes += MERKLE_HASH_BYTES;
                hashes += 1;
                response_hashes += 1;
                if left.hash(depth + 1, child) != right.hash(depth + 1, child) {
                    next.push(child);
                }
            }
        }
        stages.push(RepairStage::MerkleExchange {
            depth: (depth + 1) as u32,
            request_prefixes: request_prefixes as u64,
            request_bytes: request_bytes as u64,
            response_hashes: response_hashes as u64,
            response_bytes: (response_hashes * MERKLE_HASH_BYTES) as u64,
        });
        rounds += 1;
        mismatching = next;
    }

    let recovered = mismatching;
    if !recovered.is_empty() {
        let request_bytes = recovered.len() * 8;
        let returned_rows = recovered
            .iter()
            .filter(|key| right.rows.contains_key(key))
            .count();
        let response_bytes = returned_rows * SYMBOL_BYTES;
        bytes += request_bytes + response_bytes;
        stages.push(RepairStage::MerkleFetch {
            request_keys: recovered.len() as u64,
            request_bytes: request_bytes as u64,
            returned_rows: returned_rows as u64,
            response_bytes: response_bytes as u64,
        });
        rounds += 1;
    }

    let trace = RepairTrace::new(RepairStrategy::Merkle, stages);
    assert_eq!(trace.total_byte_variants(), vec![bytes as u64]);
    (
        Report {
            bytes,
            units: hashes,
            rounds,
            elapsed: started.elapsed(),
            trace,
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

fn run_case(n: usize, d: usize, scenario: Scenario, seed: u64) -> (Corpus, CaseResult) {
    let corpus = corpus(n, d, scenario, seed);

    let rbsr_setup_started = Instant::now();
    let left_map = fingerprint_map(&corpus.left);
    let right_map = fingerprint_map(&corpus.right);
    let rbsr_setup = rbsr_setup_started.elapsed();
    let (rbsr, rbsr_time, rbsr_diff, rbsr_trace) = rbsr_report(&left_map, &right_map);
    assert_eq!(rbsr_diff, corpus.expected_diff);

    let (riblt_setup, riblt, riblt_diff) = riblt_report(&corpus.left, &corpus.right);
    assert_eq!(riblt_diff, corpus.expected_diff);

    let merkle_setup_started = Instant::now();
    let left_merkle = RadixMerkle::new(&corpus.left);
    let right_merkle = RadixMerkle::new(&corpus.right);
    let merkle_setup = merkle_setup_started.elapsed();
    let (merkle, merkle_diff) = merkle_report(&left_merkle, &right_merkle);
    assert_eq!(merkle_diff, corpus.expected_diff);

    let workload = format!("n={n}-d={d}-{scenario}-symbol=16-radix=16");
    let revision = env::var("RECONCILE_BENCH_REVISION").unwrap_or_default();
    let arms = [
        (
            "rbsr-ftm",
            "rbsr",
            revision.as_str(),
            rbsr_setup,
            rbsr_time,
            rbsr_bytes(&rbsr),
        ),
        (
            "riblt",
            "do-riblt",
            "1.0.2",
            riblt_setup,
            riblt.elapsed,
            riblt.bytes,
        ),
        (
            "merkle",
            "radix-merkle",
            revision.as_str(),
            merkle_setup,
            merkle.elapsed,
            merkle.bytes,
        ),
    ]
    .into_iter()
    .map(|(id, implementation, version, setup, session, bytes)| {
        Arm::new(
            id,
            implementation,
            version,
            vec![
                elapsed(
                    LifecyclePhase::InitialArchitectureBuild,
                    CostOwner::Addon,
                    setup,
                ),
                elapsed(LifecyclePhase::SessionWork, CostOwner::Protocol, session),
                protocol_bytes(bytes, &format!("{id}-source-byte-model"), &workload),
            ],
        )
    })
    .collect();
    write_case_from_env(
        Case {
            target: "state-repair-mutations",
            workload: &workload,
            seed,
            records: [
                ("left".to_owned(), corpus.left.len()),
                ("right".to_owned(), corpus.right.len()),
            ]
            .into(),
            symmetric_difference: Some(corpus.set_difference_symbols),
        },
        arms,
    );
    write_repair_trace_from_env(
        "state-repair-mutations",
        &workload,
        seed,
        "rbsr-ftm",
        &rbsr_trace,
    );
    write_repair_trace_from_env(
        "state-repair-mutations",
        &workload,
        seed,
        "riblt",
        &riblt.trace,
    );
    write_repair_trace_from_env(
        "state-repair-mutations",
        &workload,
        seed,
        "merkle",
        &merkle.trace,
    );
    (
        corpus,
        CaseResult {
            rbsr_setup,
            rbsr,
            rbsr_time,
            riblt_setup,
            riblt,
            merkle_setup,
            merkle,
        },
    )
}

fn ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn main() {
    let n = env_usize("RECONCILE_MUTATION_N", DEFAULT_N);
    let seed = env_u64("RECONCILE_MUTATION_SEED", DEFAULT_SEED);
    let sweep = divergence_sweep();
    assert!(n > 0);

    println!(
        "[mutation-repair] n={n} baseline_even_keys=true merkle=fixed-u64-radix fanout={MERKLE_FANOUT}"
    );

    for d in sweep {
        assert!(d <= n);
        for scenario in Scenario::ALL {
            let (corpus, case) = run_case(n, d, scenario, seed);
            println!(
                "[mutation-repair] d={d} scenario={scenario} left_n={} right_n={} set_diff_symbols={} | RBSR bytes={} ranges={} idlist={} setup={:.3}ms repair={:.3}ms | RIBLT bytes={} coded={} setup={:.3}ms repair={:.3}ms | Merkle bytes={} hashes={} rounds={} setup={:.3}ms repair={:.3}ms",
                corpus.left.len(),
                corpus.right.len(),
                corpus.set_difference_symbols,
                rbsr_bytes(&case.rbsr),
                case.rbsr.ranges,
                case.rbsr.enumerated_elements,
                ms(case.rbsr_setup),
                ms(case.rbsr_time),
                case.riblt.bytes,
                case.riblt.units,
                ms(case.riblt_setup),
                ms(case.riblt.elapsed),
                case.merkle.bytes,
                case.merkle.units,
                case.merkle.rounds,
                ms(case.merkle_setup),
                ms(case.merkle.elapsed),
            );
        }
    }
}
