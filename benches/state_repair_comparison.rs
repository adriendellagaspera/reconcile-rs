// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Prepared-state repair comparison: RBSR vs Rateless IBLT vs Merkle.
//
// Every strategy sees the same sorted key-value manifest. Both replicas contain the same n keys;
// exactly d values have a different 64-bit digest. The business-level answer is therefore the
// same d divergent keys for every strategy. A changed row is two set symbols for RIBLT:
// (key, old_digest) exists only on the left and (key, new_digest) only on the right.
//
// The benchmark measures divergence discovery metadata only. The application payload fetched after
// discovery is intentionally excluded and would be identical regardless of the discovery method.
// Index construction is also excluded from the timed repair section: FingerprintTreeMap and the
// Merkle tree are built before timing; the RIBLT encoder object is constructed before timing and
// produces its rateless coded-symbol stream on demand.
//
// Defaults:
//   n = 100_000 rows
//   d = 1, 10, 100, 1_000, 10_000 divergent keys
//   Merkle fanout = 16
//
// Overrides:
//   RECONCILE_STATE_REPAIR_N=1000000
//   RECONCILE_STATE_REPAIR_D=1,10,100,1000
//
// Run with `cargo bench --bench state_repair_comparison`.

use std::collections::BTreeSet;
use std::env;
use std::time::{Duration, Instant};

use devkit::protocol_cost::{reconcile, Cost};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut};
use riblt::{RatelessIBLT, Symbol, UnmanagedRatelessIBLT};
use rsos::FingerprintTreeMap;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: &[usize] = &[1, 10, 100, 1_000, 10_000];
const MERKLE_FANOUT: usize = 16;
const SESSION_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const MERKLE_HASH_BYTES: usize = 32;
const MERKLE_INDEX_BYTES: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
struct KvSymbol {
    key: u64,
    digest: u64,
}

impl Symbol for KvSymbol {
    const BYTE_ARRAY_LENGTH: usize = SYMBOL_BYTES;

    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0; Self::BYTE_ARRAY_LENGTH];
        bytes[..8].copy_from_slice(&self.key.to_le_bytes());
        bytes[8..].copy_from_slice(&self.digest.to_le_bytes());
        bytes
    }

    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        assert_eq!(bytes.len(), Self::BYTE_ARRAY_LENGTH);
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
    report: Report,
    logical_bytes: usize,
    recovered_keys: Vec<u64>,
}

struct MerkleReport {
    report: Report,
    hashes_sent: usize,
    recovered_keys: Vec<u64>,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |raw| {
        raw.parse()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
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

fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(17)
        ^ 0xd6e8_feb8_6659_fd93
}

fn changed_digest(key: u64) -> u64 {
    base_digest(key) ^ 0xa5a5_5a5a_d3c3_b4b4
}

fn divergent_keys(n: usize, d: usize) -> Vec<u64> {
    assert!(d > 0, "d must be non-zero");
    assert!(d <= n, "d must not exceed n");
    let mut keys = Vec::with_capacity(d);
    for i in 0..d {
        // Midpoint sampling gives d unique, evenly distributed keys for every d <= n.
        keys.push((((2 * i + 1) * n) / (2 * d)) as u64);
    }
    keys
}

fn corpus(n: usize, d: usize) -> Corpus {
    let expected_diff = divergent_keys(n, d);
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

fn rbsr_report(left: &FingerprintTreeMap<u64, u64>, right: &FingerprintTreeMap<u64, u64>) -> (Cost, Duration) {
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let mut price = |_key| vec![SYMBOL_BYTES];
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let started = Instant::now();
    let cost = reconcile(left, right, &policy, Some(&mut price), &mut rng);
    (cost, started.elapsed())
}

fn riblt_report(left: Vec<KvSymbol>, right: Vec<KvSymbol>) -> RibltReport {
    let mut local = RatelessIBLT::new(left);
    let mut remote = RatelessIBLT::new(right);
    let mut received: UnmanagedRatelessIBLT<KvSymbol> = UnmanagedRatelessIBLT::new();
    let max_symbols = 1_024 + 20 * local.coded_symbols.len().max(remote.coded_symbols.len()).max(1);

    let started = Instant::now();
    let mut serialized_bytes = 0;
    let mut logical_bytes = 0;
    for index in 0..max_symbols {
        let coded = remote.get_coded_symbol(index);
        serialized_bytes += bincode::serialize(&coded)
            .expect("serializing a RIBLT coded symbol")
            .len();
        logical_bytes += SYMBOL_BYTES + 8 + 8;
        received.add_coded_symbol(&coded);

        let mut collapsed = local.collapse(&received);
        let peeled = collapsed.peel_all_symbols();
        if collapsed.is_empty() {
            let mut recovered_keys: Vec<_> = peeled
                .into_iter()
                .filter_map(|item| match item {
                    riblt::Local(symbol) | riblt::Remote(symbol) => Some(symbol.key),
                    riblt::NotPeelable => None,
                })
                .collect();
            recovered_keys.sort_unstable();
            recovered_keys.dedup();
            return RibltReport {
                report: Report {
                    bytes: serialized_bytes,
                    units: index + 1,
                    messages: 1,
                    rounds: 1,
                    elapsed: started.elapsed(),
                },
                logical_bytes,
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
        let mut levels = vec![
            rows.iter()
                .map(|&(key, digest)| leaf_hash(key, digest))
                .collect::<Vec<_>>(),
        ];
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
    // The responder first sends its root. If it differs, each following round requests all children
    // of the mismatching nodes at the next level. Requests carry one u64 node index each.
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
        messages += 1; // batched request for child hashes

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
        messages += 1; // batched child-hash response
        rounds += 1;
        mismatching = next;
    }

    // Leaf hashes identify the positions, but not the remote row digest itself. Fetch one fixed
    // (key,digest) symbol per mismatching leaf so this has the same discovery output as RBSR/RIBLT.
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

fn main() {
    let n = env_usize("RECONCILE_STATE_REPAIR_N", DEFAULT_N);
    let sweep = divergence_sweep();
    assert!(!sweep.is_empty(), "at least one d is required");

    println!(
        "[state-repair] n={n}; same aligned key universe; value digest differs on d keys; payload transfer excluded"
    );
    println!(
        "[state-repair] RIBLT bytes are exact bincode coded-symbol bytes; RBSR prices each enumerated row symbol at {SYMBOL_BYTES} B; Merkle uses {MERKLE_HASH_BYTES} B BLAKE3 hashes and fanout {MERKLE_FANOUT}"
    );

    for d in sweep {
        let corpus = corpus(n, d);

        let left_map = fingerprint_map(&corpus.left_rows);
        let right_map = fingerprint_map(&corpus.right_rows);
        let (rbsr, rbsr_elapsed) = rbsr_report(&left_map, &right_map);
        let rbsr_bytes = rbsr
            .total_bytes()
            .first()
            .copied()
            .unwrap_or(rbsr.refinement_bytes);

        let riblt = riblt_report(symbols(&corpus.left_rows), symbols(&corpus.right_rows));
        assert_eq!(
            riblt.recovered_keys, corpus.expected_diff,
            "RIBLT recovered a different business-key set"
        );

        let left_merkle = MerkleTree::new(&corpus.left_rows);
        let right_merkle = MerkleTree::new(&corpus.right_rows);
        let merkle = merkle_report(&left_merkle, &right_merkle);
        assert_eq!(
            merkle.recovered_keys, corpus.expected_diff,
            "Merkle recovered a different business-key set"
        );

        println!(
            "[state-repair] d={d:>6} | RBSR bytes={rbsr_bytes:>9}, msg={:>3}, ranges={:>7}, idlist={:>7}, time={:>9.3} ms | RIBLT bytes={:>9} (logical={:>9}), coded={:>7}, time={:>9.3} ms | Merkle bytes={:>9}, hashes={:>7}, msg={:>3}, rounds={:>2}, time={:>9.3} ms",
            rbsr.messages,
            rbsr.ranges,
            rbsr.enumerated_elements,
            rbsr_elapsed.as_secs_f64() * 1_000.0,
            riblt.report.bytes,
            riblt.logical_bytes,
            riblt.report.units,
            riblt.report.elapsed.as_secs_f64() * 1_000.0,
            merkle.report.bytes,
            merkle.hashes_sent,
            merkle.report.messages,
            merkle.report.rounds,
            merkle.report.elapsed.as_secs_f64() * 1_000.0,
        );
    }
}
