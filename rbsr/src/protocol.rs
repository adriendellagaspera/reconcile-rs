// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The protocol driver: [`initial_ranges`], [`protocol_round`], and the [`RangeAggregate`] wire
//! type they exchange — generic over [`RsosView`], never over a concrete store.
//! Split across siblings by concern: `bounds` owns `StartBound`/`EndBound`'s conversion to
//! [`std::ops::Bound`] and `KeyRange`'s construction and `RangeBounds` implementation; `rank` owns
//! resolving a wire range against a concrete store — admission and clamping arithmetic, and the
//! one way it can fail on a malformed segment; `range_aggregate` owns [`RangeAggregate`]'s own
//! construction and field readers; `outcome` owns [`RoundOutcome`]'s accessors and its
//! [`AddAssign`](std::ops::AddAssign). This file keeps the public type definitions (their module
//! location is their `cargo public-api`-visible path) plus the round-driving logic itself.

use std::ops::Bound;

use rand::rngs::StdRng;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::debug;

use rsos::Aggregate;

use crate::policy::{Comparison, Decision, FanOut, FixedFanOut, RefinementPolicy};
use crate::rsos_view::RsosView;

mod bounds;
mod outcome;
mod range_aggregate;
mod rank;

use rank::{BoundedRange, InvertedRange};

/// The default refinement policy used by [`protocol_round`].
const DEFAULT_POLICY: FixedFanOut = FixedFanOut::new(FanOut::NEGENTROPY);

/// The start bound of a [`RangeAggregate`] range: `Included` or `Unbounded`, never `Excluded`.
/// Narrower than `std::ops::Bound<K>` so a peer sending the third shape fails to deserialize
/// rather than reaching a runtime check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum StartBound<K> {
    Unbounded,
    Included(K),
}

/// The end bound of a [`RangeAggregate`] range: `Excluded` or `Unbounded`, never `Included`. See
/// [`StartBound`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum EndBound<K> {
    Unbounded,
    Excluded(K),
}

/// A [`RangeAggregate`]'s range. A local tuple struct, not a bare tuple, so it can implement the
/// foreign `RangeBounds` and feed [`RsosView::aggregate`] directly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct KeyRange<K>(StartBound<K>, EndBound<K>);

/// A key range paired with its [`Aggregate`], as exchanged by the protocol.
///
/// Wire encoding follows field declaration order. Changing the order or representation of these
/// fields is a wire-format change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeAggregate<K> {
    range: KeyRange<K>,
    aggregate: Aggregate,
}

/// A range whose contents this peer must send explicitly: the **IDLIST** outcome, to be fed to
/// [`rsos::Rsos::enumerate`] by the caller — the driver itself never enumerates.
/// A bare pair of [`Bound`]s, not the narrowed wire types: this is a local output, never sent.
pub type EnumerationRange<K> = (Bound<K>, Bound<K>);

/// The initial family of **active ranges**: one [`RangeAggregate`] `{(−∞, +∞), A(whole store)}`.
/// The outer range is fixed to the whole universe here; [`protocol_round`] never assumes it, so
/// partial reconciliation needs only a different starting family.
pub fn initial_ranges<K, B: RsosView<K>>(local: &B) -> Vec<RangeAggregate<K>> {
    vec![RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: local.aggregate(..),
    }]
}

/// Counts the decisions made by one [`protocol_round`].
///
/// [`AddAssign`](std::ops::AddAssign) accumulates several rounds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoundOutcome {
    skipped: usize,
    enumerated: usize,
    split: usize,
    children: usize,
    dropped_malformed: usize,
}

/// One **protocol round** under this crate's default refinement policy: classify every active range this peer was
/// asked to answer as SKIP, IDLIST or SPLIT.
/// A range's fate is read exhaustively off the outputs: `child_ranges` (SPLIT),
/// `enumeration_ranges` (IDLIST), **both** (an IDLIST against a non-empty peer range, which also
/// bounces the parent back), or neither (SKIP, or dropped as malformed and counted in
/// [`RoundOutcome::dropped_malformed`]).
/// The rule is a [`RefinementPolicy`], swappable through [`protocol_round_with_policy`] without a
/// protocol break. Split children are pairwise disjoint and cover the parent range.
pub fn protocol_round<K, B: RsosView<K>>(
    local: &B,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> RoundOutcome
where
    K: Clone,
{
    protocol_round_with_policy(
        local,
        &DEFAULT_POLICY,
        active_ranges,
        child_ranges,
        enumeration_ranges,
        rng,
    )
}

/// [`protocol_round`] with a caller-supplied refinement policy.
///
/// The policy chooses the outcome and split width. Bounds validation, rank arithmetic, partitioning,
/// and the progress guard remain enforced by the protocol driver. A non-progressing split is
/// converted to enumeration.
///
/// # RNG seam
/// `rng` is injected, never drawn from ambient/thread-local entropy: the driver's own tests stay
/// deterministic under a seeded `StdRng`, and a caller with no session-scoped RNG to reuse
/// (`Replica`/`ReadReplicaMap` share one across rounds and peers, `src/replica/dispatch.rs`) can
/// seed one fresh per call. Consumed only when a [`Decision::Split`] actually reaches the fan-out
/// below — a [`Decision::Skip`]/[`Decision::Enumerate`] round draws nothing.
pub fn protocol_round_with_policy<K, B, P>(
    local: &B,
    policy: &P,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> RoundOutcome
where
    K: Clone,
    B: RsosView<K>,
    P: RefinementPolicy + ?Sized,
{
    let mut outcome = RoundOutcome::default();
    for segment in active_ranges {
        let RangeAggregate {
            range: KeyRange(start, end),
            aggregate: remote,
        } = segment;
        // Safe before validation: `aggregate` compares, never indexes, so an inverted range is
        // simply empty.
        let local_aggregate = local.aggregate(KeyRange::new(start.clone(), end.clone()));
        // Dropping an inverted range here avoids an underflow and an out-of-bounds `select`.
        let bounded = match BoundedRange::parse(start, end, local) {
            Ok(bounded) => bounded,
            Err(InvertedRange { raw_start, raw_end }) => {
                debug!(
                    "dropping malformed segment: its start ranks after its end in this store \
                     ({raw_start} > {raw_end}), so it covers no keys and cannot be refined"
                );
                outcome.dropped_malformed += 1;
                continue;
            }
        };
        let start_index = bounded.start_index;
        let end_index = bounded.end_index;
        let BoundedRange {
            start: start_bound,
            end: end_bound,
            ..
        } = bounded;
        // The policy never sees the bounds, so it cannot decide key-dependently. `Comparison::span`
        // is read from the bundled aggregate, not from `end_index - start_index` — see
        // `RsosView`'s count-agreement law for why those two agree only for a defended backend.
        let comparison = Comparison::new(local_aggregate, remote, outcome.children);
        let span = comparison.span();
        let decision = match policy.decide(comparison) {
            Decision::Split(stride) if span > 1 && stride.get() >= span => {
                debug!(
                    "policy returned a non-progressing SPLIT (stride {} >= span {span} with \
                     span > 1); forcing IDLIST for this range instead of stalling on it",
                    stride.get()
                );
                Decision::Enumerate
            }
            other => other,
        };
        match decision {
            Decision::Skip => {
                outcome.skipped += 1;
            }
            Decision::Enumerate => {
                // IDLIST is one-directional: a non-empty peer range is bounced back advertised as
                // empty so the peer enumerates its side too.
                outcome.enumerated += 1;
                if remote.size() != 0 {
                    child_ranges.push(RangeAggregate {
                        range: KeyRange::new(start_bound.clone(), end_bound.clone()),
                        aggregate: Aggregate::ZERO,
                    });
                    outcome.children += 1;
                }
                enumeration_ranges.push((start_bound.into(), end_bound.into()));
            }
            Decision::Split(stride) => {
                outcome.split += 1;
                let stride = stride.get();
                // A fixed stride leaves at most one undersized block. Its session-random position
                // moves interior cut points without changing block count or fan-out. Use the
                // concrete resolved span rather than a policy-reported span.
                let actual_span = end_index.get() - start_index.get();
                let remainder = actual_span % stride;
                let short_block =
                    (remainder != 0).then(|| rng.gen_range(0..block_count(actual_span, stride)));
                let mut cur_bound = start_bound;
                let mut cur_index = start_index;
                let mut block = 0usize;
                loop {
                    let this_stride = if short_block == Some(block) {
                        remainder
                    } else {
                        stride
                    };
                    // `None` means the next cut would reach `end_index`: this child is the last.
                    // `Some` is in bounds for any backend by construction — see `AdmittedRank`.
                    let Some(next_index) = cur_index.cut_before(end_index, this_stride) else {
                        let range = KeyRange::new(cur_bound, end_bound);
                        // An uncut child *is* the parent, whose aggregate is already in hand.
                        let aggregate = if cur_index == start_index {
                            local_aggregate
                        } else {
                            local.aggregate(range.clone())
                        };
                        child_ranges.push(RangeAggregate { range, aggregate });
                        outcome.children += 1;
                        break;
                    };
                    let next_key = local.select(next_index.get()).clone();
                    let range = KeyRange::new(cur_bound, EndBound::Excluded(next_key.clone()));
                    let aggregate = local.aggregate(range.clone());
                    child_ranges.push(RangeAggregate { range, aggregate });
                    outcome.children += 1;
                    cur_bound = StartBound::Included(next_key);
                    cur_index = next_index;
                    block += 1;
                }
            }
        }
    }
    outcome
}

/// How many blocks a fixed `stride` cuts `actual_span` elements into: `ceil(actual_span /
/// stride)`. Factored out of the `Decision::Split` arm so its exact arithmetic (`div_ceil`, not a
/// hand-rolled `/ stride + 1`) is pinned by a direct unit test rather than only exercised through
/// the fan-out loop, where a wrong-but-in-range result is easy for a property test to miss.
fn block_count(actual_span: usize, stride: usize) -> usize {
    actual_span.div_ceil(stride)
}

#[cfg(test)]
mod tests;
