// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `reconcile_internal_testing`-only driving scaffolding for adversarial/oracle-dependent
//! `RefinementPolicy` probes: a reduced-width store and a driver that **proves** a stall instead
//! of inferring one from a round cap.
//!
//! Scope is the store and driver only — this crate carries no collision-rate tallying, confidence
//! intervals, or policy-wrapper instrumentation; that is the minimal slice
//! `shipped_policies_always_progress.rs`'s sibling invariant-13 coverage needs.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::{Bound, RangeBounds};

use rand::rngs::StdRng;
use rand::Rng;
use rsos::{digest, Aggregate, Fingerprint};

use crate::{
    initial_ranges, protocol_round_with_policy, EnumerationRange, RangeAggregate, RefinementPolicy,
    RsosView,
};

/// Round cap. Only reached by a drive that is neither settled nor *proved* stalled — see
/// [`Termination::RoundCap`], reported as its own bucket rather than folded into
/// "non-terminating".
const MAX_ROUNDS: usize = 512;

/// Universe size a drive is run at, kept as one constant so a caller's numbers are comparable
/// across probes.
pub const DRIVE_STORE_SIZE: usize = 512;

/// The low `width` bits set, `u64::MAX` at `width >= 64`.
fn mask(width: u32) -> u64 {
    if width >= 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    }
}

/// `value`, truncated to its low `width` bits.
fn masked(value: u64, width: u32) -> u64 {
    value & mask(width)
}

/// A store summarizing with `Σ mod 2^width` instead of `Σ mod 2^256` — narrow enough that a
/// collision-rate probe can observe events in a feasible trial count.
pub struct NarrowStore {
    /// The summary width, in bits.
    pub width: u32,
    /// The store's keys, sorted and deduplicated.
    pub keys: Vec<u64>,
}

impl NarrowStore {
    /// Build a store from `keys`, sorting and deduplicating them.
    pub fn new(width: u32, mut keys: Vec<u64>) -> NarrowStore {
        keys.sort_unstable();
        keys.dedup();
        NarrowStore { width, keys }
    }

    fn span<R: RangeBounds<u64>>(&self, range: &R) -> (usize, usize) {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(k) => self.keys.partition_point(|x| x < k),
            Bound::Excluded(k) => self.keys.partition_point(|x| x <= k),
        };
        let end = match range.end_bound() {
            Bound::Unbounded => self.keys.len(),
            Bound::Included(k) => self.keys.partition_point(|x| x <= k),
            Bound::Excluded(k) => self.keys.partition_point(|x| x < k),
        };
        (start, end.max(start))
    }

    /// The keys of this store that fall in `range`.
    pub fn keys_in(&self, range: &EnumerationRange<u64>) -> Vec<u64> {
        let (start, end) = self.span(range);
        self.keys[start..end].to_vec()
    }
}

impl RsosView<u64> for NarrowStore {
    fn size(&self) -> usize {
        self.keys.len()
    }

    fn aggregate<R: RangeBounds<u64>>(&self, range: R) -> Aggregate {
        let (start, end) = self.span(&range);
        let slice = &self.keys[start..end];
        let sum = slice.iter().fold(0u64, |acc, &key| {
            let limb = masked(digest(&key).0[0], self.width);
            masked(acc.wrapping_add(limb), self.width)
        });
        Aggregate::new(slice.len(), Fingerprint([sum, 0, 0, 0]))
    }

    fn rank(&self, z: &u64) -> usize {
        self.keys.partition_point(|x| x < z)
    }

    fn select(&self, r: usize) -> &u64 {
        &self.keys[r]
    }
}

/// A non-adversarial, count-balanced difference of `swap_size` elements over an `n`-key universe.
pub fn balanced_swap(rng: &mut StdRng, n: usize, swap_size: usize) -> (Vec<u64>, Vec<u64>) {
    let mut a: Vec<u64> = (0..n).map(|_| rng.gen()).collect();
    a.sort_unstable();
    a.dedup();
    let swap_size = swap_size.min(a.len());
    let mut b = a.clone();
    for _ in 0..swap_size {
        let idx = rng.gen_range(0..b.len());
        b.swap_remove(idx);
        let candidate = unique_candidate(rng, &a, &b);
        b.push(candidate);
    }
    (a, b)
}

/// Draw from `rng` until landing on a value in neither `a` nor `b`.
///
/// Generic over `Rng` (not tied to `StdRng`) so a test can drive it with a scripted sequence of
/// "random" draws instead of a real generator, to exercise the rejection loop deterministically.
fn unique_candidate<R: Rng + ?Sized>(rng: &mut R, a: &[u64], b: &[u64]) -> u64 {
    let mut candidate: u64 = rng.gen();
    while a.contains(&candidate) || b.contains(&candidate) {
        candidate = rng.gen();
    }
    candidate
}

/// How a drive ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Termination {
    /// The active family emptied: a fixed point.
    Settled,
    /// A `(parity, active family)` state recurred. The drive is a deterministic map on that
    /// state, so this is a **proof** the drive never terminates, with the cycle's length.
    Stalled {
        /// How many rounds separate the state's first occurrence from its recurrence.
        cycle_length: usize,
    },
    /// Neither, within [`MAX_ROUNDS`] — the honest "no verdict" bucket. A non-empty count here
    /// means the round cap, not the mechanism, decided the outcome.
    RoundCap,
}

/// One drive's outcome.
pub struct Drive<K> {
    /// How the drive ended.
    pub termination: Termination,
    /// Every range either side enumerated (IDLIST) over the course of the drive.
    pub enumerated: Vec<EnumerationRange<K>>,
    /// How many range comparisons the drive made in total.
    pub comparisons: u64,
    /// How many protocol rounds the drive ran.
    pub rounds: usize,
}

/// The visited-state table: `(responder parity, state hash) -> (round, exact state)`. The exact
/// state is kept so a hash collision cannot manufacture a false stall.
type VisitedStates = HashMap<(bool, u64), Vec<(usize, Vec<RangeAggregate<u64>>)>>;

/// A cheap hash of an active family, used only to *index* the visited-state table; every hit is
/// confirmed with `==`, so a hash collision cannot manufacture a false stall.
fn state_hash(active: &[RangeAggregate<u64>]) -> u64 {
    let mut hasher = DefaultHasher::new();
    // Every field #289 made public, so a state that hashes equal really is the same state; the
    // exact confirmation below then makes a hash collision harmless rather than merely unlikely.
    for segment in active {
        segment.start_bound().hash(&mut hasher);
        segment.end_bound().hash(&mut hasher);
        let aggregate = segment.aggregate();
        aggregate.size().hash(&mut hasher);
        aggregate.fingerprint().0.hash(&mut hasher);
    }
    hasher.finish()
}

/// Whether a previously-seen `state` is exactly the current `active` family — the confirmation
/// [`state_hash`]'s comment promises, so a hash collision cannot manufacture a false stall.
fn state_recurred(state: &[RangeAggregate<u64>], active: &[RangeAggregate<u64>]) -> bool {
    state == active
}

/// Reconcile `a` against `b` under `policy`, alternating which peer answers, until the active
/// family empties, a state recurs, or [`MAX_ROUNDS`] rounds pass.
pub fn drive<P: RefinementPolicy>(a: &NarrowStore, b: &NarrowStore, policy: &P) -> Drive<u64> {
    drive_pair(a, b, policy, policy)
}

/// [`drive`] with a **policy per peer**. A refinement policy is a purely local choice this crate
/// never negotiates (`ARCHITECTURE.md` §3.1), so the two sides can disagree — and progress is a
/// *joint* property, which makes "does one bad peer suffice?" a different question from "do two?".
pub fn drive_pair<A: RefinementPolicy, B: RefinementPolicy>(
    a: &NarrowStore,
    b: &NarrowStore,
    policy_a: &A,
    policy_b: &B,
) -> Drive<u64> {
    let mut active = initial_ranges(a);
    let (mut responder, mut advertiser) = (b, a);
    let mut enumerated = Vec::new();
    let mut comparisons = 0u64;
    let mut rounds = 0;
    let mut responder_is_b = true;

    // parity -> hash -> the states already seen at that parity, kept for exact confirmation.
    let mut seen: VisitedStates = HashMap::new();

    let termination = loop {
        if active.is_empty() {
            break Termination::Settled;
        }
        if rounds >= MAX_ROUNDS {
            break Termination::RoundCap;
        }
        let key = (responder_is_b, state_hash(&active));
        let bucket = seen.entry(key).or_default();
        if let Some((first_round, _)) = bucket
            .iter()
            .find(|(_, state)| state_recurred(state, &active))
        {
            break Termination::Stalled {
                cycle_length: cycle_length(rounds, *first_round),
            };
        }
        bucket.push((rounds, active.clone()));

        let mut children = Vec::new();
        let mut enumerations = Vec::new();
        // `responder_is_b` still names the answering peer, so it selects that peer's own policy.
        let outcome = if responder_is_b {
            protocol_round_with_policy(
                responder,
                policy_b,
                active,
                &mut children,
                &mut enumerations,
            )
        } else {
            protocol_round_with_policy(
                responder,
                policy_a,
                active,
                &mut children,
                &mut enumerations,
            )
        };
        comparisons += round_comparisons(
            outcome.skipped(),
            outcome.enumerated(),
            outcome.split(),
            outcome.dropped_malformed(),
        );
        enumerated.append(&mut enumerations);
        active = children;
        rounds += 1;
        std::mem::swap(&mut responder, &mut advertiser);
        responder_is_b = other_responder(responder_is_b);
    };

    Drive {
        termination,
        enumerated,
        comparisons,
        rounds,
    }
}

/// One round's contribution to the drive's total comparison count: every range that round
/// classified, regardless of what became of it.
fn round_comparisons(
    skipped: usize,
    enumerated: usize,
    split: usize,
    dropped_malformed: usize,
) -> u64 {
    (skipped + enumerated + split + dropped_malformed) as u64
}

/// How many rounds separate a recurring state's first occurrence from its recurrence.
fn cycle_length(current_round: usize, first_round: usize) -> usize {
    current_round - first_round
}

/// The peer that answers next, given who just answered: alternation, not a fixed schedule.
fn other_responder(responder_is_b: bool) -> bool {
    !responder_is_b
}

#[cfg(test)]
mod tests;
