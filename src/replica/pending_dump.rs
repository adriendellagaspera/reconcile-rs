// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cmp::Ordering;
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::Bound;

use rbsr::EnumerationRange;

use crate::bounds::{Key, Value};

use super::pacing::DumpChannel;
use super::Replica;

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Stash a differences batch that lost the per-peer dump-slot race.
    ///
    /// Equivalent or overlapping retries are kept as one range union rather than multiplying
    /// pending enumeration work. The active task for this peer drains the stash before releasing
    /// its slot; a peer stalled only by the global budget consumes it on its next admitted retry.
    pub(super) fn stash_pending_dump(
        &self,
        channel: DumpChannel,
        peer: SocketAddr,
        ranges: Vec<EnumerationRange<K>>,
    ) {
        let stash = match channel {
            DumpChannel::Dated => &self.pending_dumps,
            DumpChannel::ValueOnly => &self.pending_value_dumps,
        };
        let mut pending = stash.write();
        let peer_ranges = pending.entry(peer).or_default();
        peer_ranges.extend(ranges);
        coalesce_ranges(peer_ranges);
    }

    /// Merge work previously stashed for peer/channel into a newly admitted retry.
    ///
    /// A peer that lost only the global dump-slot race has no active task of its own to drain the
    /// stash. Consuming it before the retry enumerates prevents the same range from being
    /// materialized once for the fresh comparison and again from the old pending entry.
    pub(super) fn take_pending_dump(
        &self,
        channel: DumpChannel,
        peer: SocketAddr,
        mut ranges: Vec<EnumerationRange<K>>,
    ) -> Vec<EnumerationRange<K>> {
        let stash = match channel {
            DumpChannel::Dated => &self.pending_dumps,
            DumpChannel::ValueOnly => &self.pending_value_dumps,
        };
        let mut pending = stash.write().remove(&peer).unwrap_or_default();
        pending.append(&mut ranges);
        coalesce_ranges(&mut pending);
        pending
    }
}

/// Merge overlapping or directly touching enumeration ranges in place.
///
/// RBSR emits half-open ranges (Included(start)..Excluded(end), with either side optionally
/// unbounded). The comparison helpers enforce that local protocol invariant instead of assigning
/// semantics to bound shapes the protocol cannot produce.
fn coalesce_ranges<K: Ord>(ranges: &mut Vec<EnumerationRange<K>>) {
    if ranges.len() < 2 {
        return;
    }

    ranges.sort_by(|(left, _), (right, _)| cmp_start_bound(left, right));
    let mut coalesced: Vec<EnumerationRange<K>> = Vec::with_capacity(ranges.len());

    for (start, end) in ranges.drain(..) {
        let Some((_, previous_end)) = coalesced.last_mut() else {
            coalesced.push((start, end));
            continue;
        };

        if ranges_overlap_or_touch(previous_end, &start) {
            if cmp_end_bound(previous_end, &end).is_lt() {
                *previous_end = end;
            }
        } else {
            coalesced.push((start, end));
        }
    }

    *ranges = coalesced;
}

fn cmp_start_bound<K: Ord>(left: &Bound<K>, right: &Bound<K>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, Bound::Included(_)) => Ordering::Less,
        (Bound::Included(_), Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right)) => left.cmp(right),
        (Bound::Excluded(_), _) | (_, Bound::Excluded(_)) => {
            unreachable!("RBSR enumeration ranges never have an excluded start")
        }
    }
}

fn cmp_end_bound<K: Ord>(left: &Bound<K>, right: &Bound<K>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, Bound::Excluded(_)) => Ordering::Greater,
        (Bound::Excluded(_), Bound::Unbounded) => Ordering::Less,
        (Bound::Excluded(left), Bound::Excluded(right)) => left.cmp(right),
        (Bound::Included(_), _) | (_, Bound::Included(_)) => {
            unreachable!("RBSR enumeration ranges never have an included end")
        }
    }
}

fn ranges_overlap_or_touch<K: Ord>(left_end: &Bound<K>, right_start: &Bound<K>) -> bool {
    match (left_end, right_start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Excluded(left), Bound::Included(right)) => left >= right,
        (Bound::Included(_), _) | (_, Bound::Excluded(_)) => {
            unreachable!("RBSR enumeration ranges are half-open")
        }
    }
}
