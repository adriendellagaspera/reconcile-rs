// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Protocol-level cost of one full RBSR reconciliation, under the shipped default refinement
//! policy (`rbsr::FixedFanOut` at `b = 16`): how many total wire bytes, messages, advertised
//! ranges, datagrams and local RSOS queries two peers spend to resolve a difference of size `d` in
//! a store of size `n`, how that changes when the differences cluster instead of scattering, and
//! how it moves with the size of a stored value.
//!
//! **One unit: total wire bytes.** Refinement bytes and the values an IDLIST enumeration ships are
//! two halves of one quantity, so both are summed here at four payload sizes `V` (`VALUE_SIZES`) —
//! the axis `system`'s `memory_footprint` already varies. The breakdown is still printed under each
//! total, because it says *why* the run landed where it did.
//!
//! One-way messages stay a separate column on purpose: no byte total prices a round trip. This
//! target runs at RTT ≈ 0, so weigh that column by your own — at the rate `benches/system.rs`'s
//! injected-RTT lane measures, one RTT per round trip with no hidden multiplier
//! (`benches/README.md`).
//!
//! **Why one drive prices every `V`.** Both peers assign the same value to the same key, so equal
//! key sets have equal aggregates whatever the payload is, and every SKIP/IDLIST/SPLIT decision
//! reads aggregates alone: the *decisions* — messages, ranges, enumerations, elements, queries —
//! are identical at every payload size, and only the per-element wire cost moves. So the drive runs
//! once, over a `u64`-valued store, and each enumerated element is priced by encoding the dated
//! cell the transport really ships for it, `(K, Entry<Timestamp, Vec<u8>>)`
//! (`src/replica.rs`'s `Message::Update`), through the transport's own encoder. That is measured
//! rather than argued: `payload_size_does_not_move_the_trace` drives the same case over a `u64`, an
//! 8-byte and a 4 KB payload and compares before any table is printed. It also buys the 4 KB
//! column at `n = 10⁶`, which materializing 4 GB of payload twice could not.
//!
//! Both byte columns are payload before framing: neither carries the one-byte `Message` variant tag
//! the transport prepends per item, nor the authenticator's per-datagram overhead.
//!
//! Unlike `bench` (structure micro-benchmarks) and `system` (end-to-end over `ReplicatedMap`), this
//! target drives the protocol driver directly, through the same two crates a downstream consumer
//! would use without the facade: `rsos` for the store and `rbsr` for the round. It needs no feature
//! gate and no runtime — a reconciliation is a pure function of two stores.
//!
//! Reproduction and interpretation: `benches/README.md`. Not run in CI (only compile-checked); run
//! locally with `cargo bench --bench protocol`.

use std::hint::black_box;

use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
};
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Serialize;

use devkit::protocol_cost::{reconcile, Cost, Counting};
use lww_register::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use lww_register::Entry;
use rbsr::{FanOut, FixedFanOut, RefinementPolicy};
use rsos::{FingerprintTreeMap, Rsos};

/// Store sizes swept by the cost report (log scale). Capped at 10⁶: the point is the growth rate of
/// the exchanged volume, and two 10⁷-entry trees would dominate the benchmark's own runtime with
/// setup rather than measurement.
const SIZES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

/// Value payload sizes every total is reported at, in bytes: `system`'s `memory_footprint` axis,
/// extended to 4 KB — past that a single value approaches the datagram ceiling (README,
/// "Value-size ceiling"). The axis exists because a policy's two halves are priced against each
/// other *through* it: refinement bytes do not move with `V`, an enumerated element does.
const VALUE_SIZES: [usize; 4] = [8, 64, 512, 4096];

/// When the priced writes happened, in milliseconds since the Unix epoch (2026-08-14). A stamp's
/// two `u64`s are varints, so a zeroed clock would encode in two bytes where a real one takes
/// eighteen — pricing an enumerated element far below what it costs. Fixed, not read from the
/// clock, so the report stays reproducible.
const WRITE_INSTANT_MS: u64 = 1_786_752_000_000;

/// The identity stamping those writes, of the shape `Replica::new` mints
/// (`NodeId::new(rand::random())`) — a full-width value, again because varints make small ones
/// unrepresentative. Fixed for reproducibility.
const NODE_ID: u64 = 0xfeed_face_dead_beef;

/// How the `d` differing keys are laid out: scattered forces every subtree to refine, clustered
/// confines the work to one descent — the axis where `√m` and a fixed `b` differ most.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Clustering {
    /// Spread evenly, so the differences land in distinct subranges.
    Scattered,
    /// One contiguous block in the middle of the key space.
    Clustered,
}

impl Clustering {
    fn label(self) -> &'static str {
        match self {
            Clustering::Scattered => "scattered",
            Clustering::Clustered => "clustered",
        }
    }
}

/// The `(difference size, layout)` pairs swept against every store size. `d = 1` — the published
/// bounds' usual case — has no layout, so it appears only as `Scattered`.
const DIFFERENCES: &[(usize, Clustering)] = &[
    (1, Clustering::Scattered),
    (10, Clustering::Scattered),
    (10, Clustering::Clustered),
    (100, Clustering::Scattered),
    (100, Clustering::Clustered),
];

/// Build a store of `n` sequential entries, omitting `missing`, each key carrying `value(key)`.
/// Sequential keys: the measured quantity depends on rank positions, not key distribution, and
/// stays reproducible without a PRNG.
fn store_of<V: Serialize + Clone>(
    n: usize,
    missing: &[u64],
    value: impl Fn(u64) -> V,
) -> FingerprintTreeMap<u64, V> {
    let mut map = FingerprintTreeMap::new();
    for key in 0..n as u64 {
        if !missing.contains(&key) {
            map.insert(key, value(key));
        }
    }
    map
}

/// The store every table is driven over. Its values are `u64` rather than dated cells because the
/// decisions do not depend on them — see the module docs, and
/// `payload_size_does_not_move_the_trace`.
fn store(n: usize, missing: &[u64]) -> FingerprintTreeMap<u64, u64> {
    store_of(n, missing, |key| key.wrapping_mul(2_654_435_761))
}

/// The `d` keys withheld from the second store, laid out according to `clustering`.
fn missing_keys(n: usize, d: usize, clustering: Clustering) -> Vec<u64> {
    match clustering {
        Clustering::Scattered => (1..=d as u64)
            .map(|i| (n as u64 / (d as u64 + 1)) * i)
            .collect(),
        // Centred so the block is not adjacent to either end of the key space, where a partition's
        // outermost child would absorb it for free.
        Clustering::Clustered => {
            let start = (n / 2 - d / 2) as u64;
            (start..start + d as u64).collect()
        }
    }
}

/// The stamp the entry under `key` carries: one HLC reading per write, at a plausible instant.
fn stamp(key: u64) -> Timestamp {
    Timestamp::new(
        Hlc::new(
            PhysicalTime::from_millis(WRITE_INSTANT_MS + key),
            LogicalCounter::ZERO,
        ),
        NodeId::new(NODE_ID),
    )
}

/// The dated cell one key is stored and shipped as, at payload size `value_bytes`: the register
/// cell `ReplicatedMap` stores (`src/replica.rs`'s `FingerprintTreeMap<K, Entry<Timestamp, V>>`).
///
/// The payload is a `Vec<u8>` rather than a `[u8; V]` because that is what a deployment can
/// actually store: `lww_register::Value` demands `Serialize`, which `serde` implements for arrays
/// only up to 32 elements. It costs the wire a length varint an array would not carry — one byte up
/// to 250, three beyond — which is part of the price, not an artifact of the harness.
fn dated_cell(key: u64, value_bytes: usize) -> Entry<Timestamp, Vec<u8>> {
    Entry::present(stamp(key), vec![key as u8; value_bytes])
}

/// What one enumerated element costs on the wire, one entry per [`VALUE_SIZES`] payload size:
/// `Message::Update`'s payload (`src/replica.rs`), through the transport's own encoder.
///
/// Measured per element rather than derived from a per-entry constant — bincode's varints make the
/// key and the stamp cost what their values happen to cost — and read straight off [`VALUE_SIZES`],
/// so the reported sizes and the priced cells cannot drift apart.
fn element_bytes(key: u64, scratch: &mut Vec<u8>) -> [usize; VALUE_SIZES.len()] {
    VALUE_SIZES.map(|value_bytes| {
        scratch.clear();
        gossip::bincode::encode(&(key, dated_cell(key, value_bytes)), scratch)
            .expect("encoding an entry into an in-memory buffer cannot fail");
        scratch.len()
    })
}

// The `Queries`/`Counting`/`Cost`/`Decisions` types and the `reconcile` driver itself moved to
// `devkit::protocol_cost` (#524): generic over any `rsos::Rsos` backend and any
// `rbsr::RefinementPolicy`, with no dependency on this crate's own wire format. `element_bytes`
// is what wires this repository's dated-cell payload into it, via `reconcile`'s `price_element`
// closure — see `counted_reconcile`.

/// The premise of the whole value-size axis, checked instead of asserted: one drive can price every
/// payload size because no decision reads the payload.
///
/// Same keys, three value types — the `u64` every table is driven over, and the dated cells at both
/// ends of [`VALUE_SIZES`] — so the comparison covers the substitution the report actually makes.
/// Decisions must match exactly. Refinement *bytes* are held to a tolerance instead, because a
/// different payload gives a different fingerprint and bincode spends four bytes fewer on a limb
/// that happens to fall below 2³²: an equality assertion would be sound about one run in a hundred
/// thousand, and the quantity it would be wrong about is a handful of bytes in tens of thousands.
fn payload_size_does_not_move_the_trace() {
    const N: usize = 10_000;
    const D: usize = 10;
    /// Refinement-byte drift a differing fingerprint may cause. Two orders of magnitude above what
    /// the varint arithmetic above can produce at this `n`, and far below anything a changed
    /// decision could hide in.
    const TOLERANCE: f64 = 0.001;

    let missing = missing_keys(N, D, Clustering::Scattered);
    let plain = (store(N, &[]), store(N, &missing));
    // Both ends of the axis, since a payload that moved the trace would move it most where it is
    // widest.
    let dated = [VALUE_SIZES[0], VALUE_SIZES[VALUE_SIZES.len() - 1]].map(|value_bytes| {
        (
            value_bytes,
            store_of(N, &[], |key| dated_cell(key, value_bytes)),
            store_of(N, &missing, |key| dated_cell(key, value_bytes)),
        )
    });

    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let reference = counted_reconcile(&plain.0, &plain.1, &policy);
    let mut worst_drift = 0.0f64;
    for (value_bytes, full, holed) in &dated {
        let cost = counted_reconcile(full, holed, &policy);
        assert_eq!(
            cost.decisions(),
            reference.decisions(),
            "default policy: a {value_bytes} B payload changed the refinement trace — one drive \
             cannot price every value size"
        );
        let drift = (cost.refinement_bytes as f64 - reference.refinement_bytes as f64).abs()
            / reference.refinement_bytes as f64;
        assert!(
            drift <= TOLERANCE,
            "default policy: a {value_bytes} B payload moved the refinement traffic by {:.3} % \
             ({} B against {} B) — more than a fingerprint's varint width can explain",
            drift * 100.0,
            cost.refinement_bytes,
            reference.refinement_bytes
        );
        worst_drift = worst_drift.max(drift);
    }
    println!(
        "[protocol] payload independence verified at n={N} d={D} scattered, default policy: \
         identical decisions over u64 and {:?} B values, refinement bytes within {:.3} % \
         (tolerated: {:.3} %)",
        dated.map(|(value_bytes, _, _)| value_bytes),
        worst_drift * 100.0,
        TOLERANCE * 100.0
    );
}

/// One `V=… total` cell per payload size.
fn totals(cost: &Cost) -> String {
    VALUE_SIZES
        .iter()
        .zip(cost.total_bytes())
        .map(|(v, total)| format!("V={v:<4} {total:>10}"))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The refinement/IDLIST breakdown under a total: why the run landed there.
fn breakdown(cost: &Cost) -> String {
    format!(
        "refine {bytes:>9} B / {ranges:>6} r / {messages:>3} msgs / {datagrams:>3} dgrams \
         / {fragments:>5} frags | widest {largest:>5} r = {largest_bytes:>7} B \
         | idlist {enumerations:>4} r / {elements:>7} elem \
         | agg {aggregate:>7} rank {rank:>7} sel {select:>7}",
        bytes = cost.refinement_bytes,
        ranges = cost.ranges,
        messages = cost.messages,
        datagrams = cost.datagrams,
        fragments = cost.fragments,
        largest = cost.largest_message,
        largest_bytes = cost.largest_message_bytes,
        enumerations = cost.enumerations,
        elements = cost.enumerated_elements,
        aggregate = cost.queries.aggregate,
        rank = cost.queries.rank,
        select = cost.queries.select,
    )
}

/// One priced reconciliation, with both peers' local query counts folded in: the table path.
///
/// `rng` seeds fresh here rather than being threaded from the caller: every call in this file
/// prices one independent drive, and a fixed seed keeps a printed/asserted report reproducible
/// across runs (`benches/README.md`), which a shared, monotonically-advancing stream would not.
fn counted_reconcile<S: Rsos<u64>>(a: &S, b: &S, policy: &dyn RefinementPolicy) -> Cost {
    let (counted_a, counted_b) = (Counting::new(a), Counting::new(b));
    let mut scratch = Vec::new();
    let mut price = |key: u64| element_bytes(key, &mut scratch).to_vec();
    let mut rng = StdRng::seed_from_u64(42);
    let mut cost = reconcile(&counted_a, &counted_b, policy, Some(&mut price), &mut rng);
    cost.queries = counted_a.queries() + counted_b.queries();
    cost
}

/// The session-random cut offset (`rbsr`'s ARCHITECTURE.md §7, "Defense against a correlated false
/// SKIP") only moves which block of a fixed-stride split is undersized — it changes no split's
/// child *count*, so it must move the round count by no more than the constant slack alternating
/// responders and one IDLIST bounce-back already cost the unshifted driver. Checked here rather
/// than asserted from first principles: this is Meyer §5.1's own claim about the mechanism, priced
/// against the real driver instead of taken on faith.
///
/// Generous on purpose — this exists to catch a regression that makes the descent no longer
/// logarithmic (a stride that stops narrowing, an off-by-one that revisits a level), not to pin an
/// exact round count the shipped policy's own tuning is free to move.
fn assert_round_bound(n: usize, d: usize, clustering: Clustering, cost: &Cost, fan_out: usize) {
    let depth = (n as f64).log(fan_out as f64).ceil().max(1.0) as usize;
    let bound = 2 * depth + 4;
    assert!(
        cost.messages <= bound,
        "n={n} d={d} {}: {} rounds exceeds the O(log_{fan_out} n) bound of {bound} \
         (log_{fan_out} {n} ~= {depth}) -- the descent no longer looks logarithmic",
        clustering.label(),
        cost.messages,
    );
}

/// Exchanged volume under the shipped default policy, printed rather than timed — exact and
/// reproducible for a given `(n, d, clustering)` — alongside the timed drive loop, the paper's
/// `T_loc`.
fn reconciliation_cost(c: &mut Criterion) {
    payload_size_does_not_move_the_trace();
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    println!(
        "[protocol] full reconciliation, u64 keys, default policy (FixedFanOut, b=16).\n\
         [protocol] first line: total wire bytes (refinement + enumerated values) per value size; \
         second: what makes it up, and the round-trip count no total prices."
    );
    for &n in SIZES {
        // The complete store is the same for every corpus at this size; only the holed one varies.
        let full = store(n, &[]);
        for &(d, clustering) in DIFFERENCES {
            let holed = store(n, &missing_keys(n, d, clustering));
            println!("[protocol] n={n} d={d} {}", clustering.label());
            let cost = counted_reconcile(&full, &holed, &policy);
            println!("[protocol]   {}", totals(&cost));
            println!("[protocol]   {}", breakdown(&cost));
            assert_round_bound(n, d, clustering, &cost, FanOut::NEGENTROPY.get());
        }
    }

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("reconciliation_drive");
    group.plot_config(plot_config);
    for &n in SIZES {
        let full = store(n, &[]);
        let holed = store(n, &missing_keys(n, 1, Clustering::Scattered));
        group.sample_size(10.max(1_000_000 / n).min(100));
        let mut rng = StdRng::seed_from_u64(42);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bencher, _| {
            bencher
                .iter(|| reconcile(black_box(&full), black_box(&holed), &policy, None, &mut rng));
        });
    }
    group.finish();
}

criterion_group!(benches, reconciliation_cost);
criterion_main!(benches);
