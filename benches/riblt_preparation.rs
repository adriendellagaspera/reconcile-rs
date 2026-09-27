// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// RIBLT product-cost benchmark.
//
// Compare the published do-riblt Encoder with its CachedEncoder when one local state reconciles
// with several remote peers. The cache is precomputed to the deepest coded-symbol index required by
// the peer corpus, then reused. Mutation cost is a full rebuild because do-riblt 1.0.2 exposes no
// public add/remove mutation API for Encoder/CachedEncoder.
//
// The cache byte model is structural, not allocator-resident memory:
//   Vec slot = size_of::<Option<Box<CodedSymbol<N>>>>()
//   non-empty coded symbol = size_of::<CodedSymbol<N>>()
// Reported cache bytes assume every cached slot owns a coded symbol, so they are an upper bound
// excluding Vec spare capacity and allocator metadata. Encoder HashMap heap usage is opaque through
// the public API and is not guessed here.
//
// Defaults:
//   n = 100_000 rows
//   d = 1_000 changed values per peer
//   peers = 1, 2, 4, 8
//
// Overrides:
//   RECONCILE_RIBLT_N=100000
//   RECONCILE_RIBLT_D=1000
//   RECONCILE_RIBLT_PEERS=1,2,4,8
//   RECONCILE_RIBLT_SEED=42
//
// Run with `cargo bench --bench riblt_preparation`.

use std::collections::BTreeSet;
use std::env;
use std::mem::size_of;
use std::time::{Duration, Instant};

use do_riblt::{CachedEncoder, CodedSymbol, Decoder, Encoder, Peeled, Symbol};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

const DEFAULT_N: usize = 100_000;
const DEFAULT_D: usize = 1_000;
const DEFAULT_PEERS: &[usize] = &[1, 2, 4, 8];
const DEFAULT_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const CODED_SYMBOL_BYTES: usize = SYMBOL_BYTES + 8;

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

struct Peer {
    rows: Vec<(u64, u64)>,
    expected_diff: Vec<u64>,
}

struct Session {
    setup: Duration,
    repair: Duration,
    coded: usize,
    recovered: Vec<u64>,
}

struct CachePreparation {
    encoder: CachedEncoder<SYMBOL_BYTES>,
    build: Duration,
    precompute: Duration,
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

fn peer_sweep() -> Vec<usize> {
    env::var("RECONCILE_RIBLT_PEERS").map_or_else(
        |_| DEFAULT_PEERS.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!("RECONCILE_RIBLT_PEERS must be comma-separated positive integers")
                    })
                })
                .collect()
        },
    )
}

fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ 0xd6e8_feb8_6659_fd93
}

fn changed_digest(key: u64, peer: usize) -> u64 {
    base_digest(key) ^ 0xa5a5_5a5a_d3c3_b4b4 ^ (peer as u64).wrapping_mul(0x517c_c1b7_2722_0a95)
}

fn base_rows(n: usize) -> Vec<(u64, u64)> {
    (0..n as u64).map(|key| (key, base_digest(key))).collect()
}

fn make_peer(base: &[(u64, u64)], d: usize, seed: u64, peer: usize) -> Peer {
    assert!(d <= base.len(), "d must not exceed n");
    let mut ranks: Vec<_> = (0..base.len()).collect();
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(peer as u64));
    ranks.shuffle(&mut rng);
    ranks.truncate(d);
    ranks.sort_unstable();

    let changed: BTreeSet<_> = ranks.iter().copied().collect();
    let rows = base
        .iter()
        .enumerate()
        .map(|(index, &(key, digest))| {
            if changed.contains(&index) {
                (key, changed_digest(key, peer))
            } else {
                (key, digest)
            }
        })
        .collect();
    let expected_diff = ranks.into_iter().map(|index| base[index].0).collect();

    Peer {
        rows,
        expected_diff,
    }
}

fn symbols(rows: &[(u64, u64)]) -> impl Iterator<Item = KvSymbol> + '_ {
    rows.iter().map(|&(key, digest)| KvSymbol { key, digest })
}

fn recovered_keys(peeled: Vec<Peeled<KvSymbol>>) -> Vec<u64> {
    let mut keys: Vec<_> = peeled
        .into_iter()
        .map(|item| match item {
            Peeled::MissingLocal(symbol) | Peeled::MissingRemote(symbol) => symbol.key,
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn run_on_demand(remote: &[(u64, u64)], peer: &Peer) -> Session {
    let setup_started = Instant::now();
    let mut encoder = Encoder::<SYMBOL_BYTES>::new(symbols(remote));
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(symbols(&peer.rows));
    let setup = setup_started.elapsed();

    let started = Instant::now();
    let mut coded = 0;
    let mut peeled = Vec::new();
    loop {
        let symbol = encoder.next().expect("RIBLT encoder is rateless");
        coded += 1;
        let (done, new_peeled) = decoder.next_symbol(symbol);
        peeled.extend(new_peeled);
        if done {
            break;
        }
    }

    Session {
        setup,
        repair: started.elapsed(),
        coded,
        recovered: recovered_keys(peeled),
    }
}

fn prepare_cache(remote: &[(u64, u64)], depth: usize) -> CachePreparation {
    assert!(depth > 0, "cache depth must be non-zero");
    let build_started = Instant::now();
    let mut encoder = CachedEncoder::<SYMBOL_BYTES>::new(symbols(remote));
    let build = build_started.elapsed();

    let precompute_started = Instant::now();
    let _ = encoder.get(depth - 1);
    let precompute = precompute_started.elapsed();

    CachePreparation {
        encoder,
        build,
        precompute,
    }
}

fn run_cached(encoder: &mut CachedEncoder<SYMBOL_BYTES>, peer: &Peer) -> Session {
    let setup_started = Instant::now();
    let mut decoder = Decoder::<SYMBOL_BYTES, KvSymbol>::new(symbols(&peer.rows));
    let setup = setup_started.elapsed();

    let started = Instant::now();
    let mut coded = 0;
    let mut peeled = Vec::new();
    loop {
        let symbol = encoder.get(coded);
        coded += 1;
        let (done, new_peeled) = decoder.next_symbol(symbol);
        peeled.extend(new_peeled);
        if done {
            break;
        }
    }

    Session {
        setup,
        repair: started.elapsed(),
        coded,
        recovered: recovered_keys(peeled),
    }
}

fn mutation_rows(base: &[(u64, u64)], kind: &str) -> Vec<(u64, u64)> {
    let mut rows = base.to_vec();
    let middle = rows.len() / 2;
    match kind {
        "update" => rows[middle].1 ^= 0x55aa_aa55_1234_5678,
        "delete" => {
            rows.remove(middle);
        }
        "insert" => rows.push((base.len() as u64, base_digest(base.len() as u64))),
        other => panic!("unknown mutation kind {other}"),
    }
    rows
}

fn duration_ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn main() {
    let n = env_usize("RECONCILE_RIBLT_N", DEFAULT_N);
    let d = env_usize("RECONCILE_RIBLT_D", DEFAULT_D);
    let seed = env_u64("RECONCILE_RIBLT_SEED", DEFAULT_SEED);
    let peers = peer_sweep();
    assert!(n > 0, "n must be non-zero");
    assert!(d > 0 && d <= n, "d must be in 1..=n");
    assert!(!peers.is_empty(), "at least one peer count is required");
    assert!(peers.iter().all(|&count| count > 0));
    let max_peers = *peers.iter().max().unwrap();

    let remote = base_rows(n);
    let peer_states: Vec<_> = (0..max_peers)
        .map(|peer| make_peer(&remote, d, seed, peer + 1))
        .collect();

    println!(
        "[riblt-preparation] n={n} d={d} max_peers={max_peers} symbol={SYMBOL_BYTES}B coded={CODED_SYMBOL_BYTES}B"
    );

    let mut on_demand = Vec::with_capacity(max_peers);
    for (index, peer) in peer_states.iter().enumerate() {
        let session = run_on_demand(&remote, peer);
        assert_eq!(session.recovered, peer.expected_diff);
        println!(
            "[riblt-on-demand] peer={} setup={:.3}ms repair={:.3}ms coded={} bytes={}",
            index + 1,
            duration_ms(session.setup),
            duration_ms(session.repair),
            session.coded,
            session.coded * CODED_SYMBOL_BYTES,
        );
        on_demand.push(session);
    }

    let cache_depth = on_demand.iter().map(|session| session.coded).max().unwrap();
    let mut cached = prepare_cache(&remote, cache_depth);
    let cache_slot = size_of::<Option<Box<CodedSymbol<SYMBOL_BYTES>>>>();
    let coded_size = size_of::<CodedSymbol<SYMBOL_BYTES>>();
    let cache_upper = cache_depth * (cache_slot + coded_size);
    println!(
        "[riblt-cache] depth={cache_depth} build={:.3}ms precompute={:.3}ms slot={}B coded={}B structural_upper={}B per_row={:.3}B",
        duration_ms(cached.build),
        duration_ms(cached.precompute),
        cache_slot,
        coded_size,
        cache_upper,
        cache_upper as f64 / n as f64,
    );

    let mut warm = Vec::with_capacity(max_peers);
    for (index, peer) in peer_states.iter().enumerate() {
        let session = run_cached(&mut cached.encoder, peer);
        assert_eq!(session.recovered, peer.expected_diff);
        assert_eq!(
            session.coded, on_demand[index].coded,
            "cached and on-demand encoders must produce the same coded-symbol prefix"
        );
        println!(
            "[riblt-cached] peer={} decoder_setup={:.3}ms warm_repair={:.3}ms coded={}",
            index + 1,
            duration_ms(session.setup),
            duration_ms(session.repair),
            session.coded,
        );
        warm.push(session);
    }

    for count in peers {
        assert!(count <= max_peers, "peer sweep must not exceed max peers");
        let on_demand_total: Duration = on_demand[..count]
            .iter()
            .map(|session| session.setup + session.repair)
            .sum();
        let warm_total: Duration = warm[..count]
            .iter()
            .map(|session| session.setup + session.repair)
            .sum();
        let cached_total = cached.build + cached.precompute + warm_total;
        println!(
            "[riblt-multipeer] peers={count} on_demand_total={:.3}ms cached_prepare={:.3}ms cached_warm_sessions={:.3}ms cached_total={:.3}ms ratio={:.3}",
            duration_ms(on_demand_total),
            duration_ms(cached.build + cached.precompute),
            duration_ms(warm_total),
            duration_ms(cached_total),
            cached_total.as_secs_f64() / on_demand_total.as_secs_f64(),
        );
    }

    for kind in ["update", "delete", "insert"] {
        let mutated = mutation_rows(&remote, kind);
        let rebuilt = prepare_cache(&mutated, cache_depth);
        println!(
            "[riblt-rebuild] kind={kind} rows={} build={:.3}ms precompute={:.3}ms total={:.3}ms",
            mutated.len(),
            duration_ms(rebuilt.build),
            duration_ms(rebuilt.precompute),
            duration_ms(rebuilt.build + rebuilt.precompute),
        );
    }

    println!(
        "[riblt-memory] source_rows={n} raw_symbol_payload={}B cache_depth={cache_depth} cache_structural_upper={}B encoder_heap=opaque-public-api",
        n * SYMBOL_BYTES,
        cache_upper,
    );
    println!(
        "[riblt-incremental] supported=false reason=do-riblt-1.0.2-Encoder-add/remove-are-private"
    );
}
