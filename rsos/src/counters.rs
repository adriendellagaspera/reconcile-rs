// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deterministic counters for the work the RSOS contract mandates, on both of its paths: the
//! *counted* half of the write-cost question `benches/contention.rs` can only put in wall-clock
//! terms (#455, #457), and the read-side descent `Aggregate(l, u)` actually performs.
//!
//! # Why counted
//!
//! Answering `Aggregate(l, u)` in `O(log n)` requires an up-to-date summary on every node from the
//! leaf to the root, so **every insert rewrites the cached aggregate of every node on its root
//! path**. That is the contract's own price, and a throughput benchmark can only report it as a
//! ratio on one machine. A count is machine-independent — the same number on a laptop and on a
//! 128-core server — so a result quoting it can be reproduced, or refuted, by someone without
//! access to the hardware that produced it.
//!
//! # The seam cannot be bypassed
//!
//! `Node::subtree` is private to `fingerprint_tree_map::node`, and every write to it goes through
//! one of that module's two setters — which is where `record_aggregate_update` is called. A new
//! aggregate-maintenance path is counted because it compiles, not because someone remembered to
//! instrument it (AGENTS.md §10).
//!
//! # The read side, and why it is two counters rather than one
//!
//! `Aggregate(l, u)` answers a fully-contained subtree from its cached summary in **O(1)** and
//! otherwise walks the range's two frontiers, scanning up to `2B - 1` entries per node. So its cost
//! is not the query count but the *shape* of that descent, and the shape needs both numbers:
//! `aggregate_early_exits` counts the O(1) returns, `aggregate_node_visits` every node entered.
//! Their ratio is the quantity of interest; either alone is not, because a query over a narrower
//! range visits fewer nodes for a reason that has nothing to do with how well the range aligns
//! with the tree's own boundaries. Reported per thread and per operation like the write counter,
//! and machine-independent for the same reason.
//!
//! # Cost when disabled
//!
//! Off unless `--cfg reconcile_internal_testing` is in `RUSTFLAGS` (#330, AGENTS.md §6). The call
//! sites carry no `#[cfg]`: they call `record_aggregate_update`, which is this module's live one or
//! its empty-bodied one, chosen by `cfg` here rather than at each call site.
//!
//! # Threading
//!
//! Counts are **per-thread** (`thread_local!` + [`Cell`](std::cell::Cell), never an atomic): a
//! shared counter would itself be a contention point, perturbing the very measurement it exists to
//! explain. Read them from a single-threaded pass — the count is deterministic, so one such pass
//! characterizes every writer count.

#[cfg(reconcile_internal_testing)]
pub use enabled::{snapshot, Counts};

#[cfg(not(reconcile_internal_testing))]
pub(crate) use disabled::{
    record_aggregate_early_exit, record_aggregate_node_visit, record_aggregate_update,
};
#[cfg(reconcile_internal_testing)]
pub(crate) use enabled::{
    record_aggregate_early_exit, record_aggregate_node_visit, record_aggregate_update,
};

/// The live half: what the crate compiles against when the `--cfg` is set.
#[cfg(reconcile_internal_testing)]
pub mod enabled {
    use std::cell::Cell;
    use std::ops::Sub;

    thread_local! {
        static AGGREGATE_UPDATES: Cell<u64> = const { Cell::new(0) };
        static AGGREGATE_EARLY_EXITS: Cell<u64> = const { Cell::new(0) };
        static AGGREGATE_NODE_VISITS: Cell<u64> = const { Cell::new(0) };
    }

    /// A reading of this thread's counters, taken by [`snapshot`].
    ///
    /// Differences are what carry meaning, so this is a [`Sub`] type, and the only way to read a
    /// count: bracket the operation under study with two snapshots and subtract. There is
    /// deliberately no reset — a counter another reader on the same thread has already bracketed is
    /// not this caller's to zero, and a difference never needed one.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct Counts {
        /// Cached subtree [`Aggregate`](crate::Aggregate)s written.
        ///
        /// One per node on the root path of the operation, plus one per node whose aggregate a
        /// split, merge or rotation had to recompute wholesale. This is the quantity a plain
        /// `BTreeMap` — same descent, no summary to maintain — scores zero on.
        pub aggregate_updates: u64,
        /// Nodes an [`aggregate`](crate::FingerprintTreeMap::aggregate) descent answered from the
        /// cached subtree summary alone, in O(1), without entering their children.
        pub aggregate_early_exits: u64,
        /// Nodes an [`aggregate`](crate::FingerprintTreeMap::aggregate) descent entered at all,
        /// early exits included. The denominator [`aggregate_early_exits`](Self::aggregate_early_exits)
        /// is only meaningful against.
        pub aggregate_node_visits: u64,
    }

    impl Sub for Counts {
        type Output = Counts;

        /// Componentwise, saturating: a difference taken against a later snapshot is a caller
        /// error, not a reason to panic in a build that only exists to be measured.
        fn sub(self, rhs: Counts) -> Counts {
            Counts {
                aggregate_updates: self.aggregate_updates.saturating_sub(rhs.aggregate_updates),
                aggregate_early_exits: self
                    .aggregate_early_exits
                    .saturating_sub(rhs.aggregate_early_exits),
                aggregate_node_visits: self
                    .aggregate_node_visits
                    .saturating_sub(rhs.aggregate_node_visits),
            }
        }
    }

    /// This thread's counters as they stand.
    #[must_use]
    pub fn snapshot() -> Counts {
        Counts {
            aggregate_updates: AGGREGATE_UPDATES.get(),
            aggregate_early_exits: AGGREGATE_EARLY_EXITS.get(),
            aggregate_node_visits: AGGREGATE_NODE_VISITS.get(),
        }
    }

    /// Counts one write of a node's cached subtree aggregate.
    #[inline]
    pub(crate) fn record_aggregate_update() {
        AGGREGATE_UPDATES.set(AGGREGATE_UPDATES.get() + 1);
    }

    /// Counts one node entered by an `aggregate` descent.
    #[inline]
    pub(crate) fn record_aggregate_node_visit() {
        AGGREGATE_NODE_VISITS.set(AGGREGATE_NODE_VISITS.get() + 1);
    }

    /// Counts one node an `aggregate` descent answered from its cached summary alone.
    #[inline]
    pub(crate) fn record_aggregate_early_exit() {
        AGGREGATE_EARLY_EXITS.set(AGGREGATE_EARLY_EXITS.get() + 1);
    }
}

/// The no-op half: what the crate compiles against when the `--cfg` is absent.
#[cfg(not(reconcile_internal_testing))]
mod disabled {
    /// Records nothing — an empty body, so every call site vanishes.
    #[inline(always)]
    pub(crate) fn record_aggregate_update() {}

    /// Records nothing — see [`record_aggregate_update`].
    #[inline(always)]
    pub(crate) fn record_aggregate_node_visit() {}

    /// Records nothing — see [`record_aggregate_update`].
    #[inline(always)]
    pub(crate) fn record_aggregate_early_exit() {}
}

/// The counted half of the write-cost question (#455, #457): This module claims to report
/// how many cached aggregates an operation maintains. These pin that claim to an independently
/// computed property of the tree, so a counter that drifts — double-counting, missing a
/// maintenance path, or straying onto the read path — fails rather than quietly reporting a
/// plausible number.
#[cfg(all(test, reconcile_internal_testing))]
mod tests {
    use super::snapshot;
    use crate::FingerprintTreeMap;

    /// The 1-based depth of the node holding `key`, walked directly rather than derived from the
    /// counter under test.
    fn depth_of<K: Ord, V>(map: &FingerprintTreeMap<K, V>, key: &K) -> usize {
        let mut node = map.root.as_ref();
        let mut depth = 1;
        loop {
            match node.keys.binary_search(key) {
                Ok(_) => return depth,
                Err(index) => {
                    let children = node.children.as_ref().expect("key is present in the tree");
                    node = &children[index];
                    depth += 1;
                }
            }
        }
    }

    /// The contract's price, stated exactly: overwriting an existing key changes no tree shape, so
    /// the aggregates it must refresh are precisely those on the key's root path — one per level,
    /// no more. This is the `O(log n)`-writes-per-write claim `SOTA.md` §2.4 item 10 rests on.
    #[test]
    fn overwriting_a_key_updates_one_aggregate_per_level_of_its_root_path() {
        let mut map = FingerprintTreeMap::new();
        for key in 0..10_000u32 {
            map.insert(key, key);
        }

        let mut depths = Vec::new();
        for probe in [0u32, 1, 1_234, 5_000, 9_999] {
            let expected = depth_of(&map, &probe);
            let before = snapshot();
            map.insert(probe, probe + 1);
            let updates = (snapshot() - before).aggregate_updates;
            assert_eq!(
                updates as usize, expected,
                "overwriting {probe} maintained {updates} aggregates, its root path has {expected} levels"
            );
            depths.push(expected);
        }

        // Guards the assertion above against passing vacuously on a one-node tree: 10 000 entries
        // in a B = 6 tree cannot all sit at the root, so at least one probe must be below it.
        assert!(
            depths.iter().any(|&d| d > 1),
            "every probe resolved at the root -- the per-level claim was never exercised"
        );
    }

    /// Reads are not writes. Were the *write* counter reachable from the query path, the
    /// write-cost figure it feeds would silently absorb the cost of `Aggregate(l, u)` — the
    /// operation the contract buys, not the one it charges for.
    ///
    /// Narrowed from `Counts::default()` to the one field it is about when the read-side counters
    /// landed: those are *supposed* to move here, and a whole-struct equality would have made this
    /// test fail for the opposite of the reason it exists.
    #[test]
    fn the_read_path_maintains_no_aggregates() {
        let mut map = FingerprintTreeMap::new();
        for key in 0..1_000u32 {
            map.insert(key, key);
        }

        let before = snapshot();
        assert_eq!(map.aggregate(..).size(), 1_000);
        assert_eq!(map.aggregate(100..200).size(), 100);
        assert_eq!(map.get(&500), Some(&500));
        assert_eq!(map.rank(&500), 500);
        assert_eq!(map.select(500), &500);
        assert_eq!(map.iter().count(), 1_000);
        assert_eq!((snapshot() - before).aggregate_updates, 0);
    }

    /// The read counters' sharpest case, and the one a misplaced call site fails: a query whose
    /// bounds contain the whole tree is answered by the root's cached summary, so the descent
    /// enters exactly one node and leaves it without looking at a child.
    ///
    /// Exact rather than a bound, because this is the O(1) the RSOS contract promises: any number
    /// above one here means the summary was recomputed instead of read.
    #[test]
    fn an_unbounded_aggregate_answers_from_the_root_without_descending() {
        let mut map = FingerprintTreeMap::new();
        for key in 0..10_000u32 {
            map.insert(key, key);
        }

        let before = snapshot();
        assert_eq!(map.aggregate(..).size(), 10_000);
        let counts = snapshot() - before;
        assert_eq!(counts.aggregate_node_visits, 1);
        assert_eq!(counts.aggregate_early_exits, 1);
    }

    /// ...and withholding a single element forces the frontier walk the root cannot answer. Pins
    /// that the early exit tracks *containment* rather than firing on entry: without this, a
    /// counter incremented unconditionally would satisfy the test above and still be wrong.
    #[test]
    fn excluding_one_element_forces_a_descent_the_root_cannot_answer() {
        let mut map = FingerprintTreeMap::new();
        for key in 0..10_000u32 {
            map.insert(key, key);
        }

        let before = snapshot();
        assert_eq!(map.aggregate(1..).size(), 9_999);
        let counts = snapshot() - before;
        assert!(
            counts.aggregate_node_visits > 1,
            "a range the root cannot answer visited {} node(s)",
            counts.aggregate_node_visits
        );
        assert!(counts.aggregate_early_exits < counts.aggregate_node_visits);
    }

    /// The structural relation the ratio is read against, over every shape a rank-cut refinement
    /// actually queries: an early exit is a node that was entered, so it can never outnumber the
    /// visits, and a query always enters at least the root.
    #[test]
    fn an_early_exit_is_always_a_visited_node() {
        let mut map = FingerprintTreeMap::new();
        for key in 0..4_096u32 {
            map.insert(key, key);
        }

        for stride in [1u32, 2, 3, 7, 16, 64, 512] {
            for start in (0..4_096).step_by((stride as usize).max(1) * 97) {
                let before = snapshot();
                map.aggregate(start..start + stride);
                let counts = snapshot() - before;
                assert!(
                    counts.aggregate_node_visits >= 1,
                    "stride={stride} start={start}: no node entered"
                );
                assert!(
                    counts.aggregate_early_exits <= counts.aggregate_node_visits,
                    "stride={stride} start={start}: {} early exits over {} visits",
                    counts.aggregate_early_exits,
                    counts.aggregate_node_visits
                );
            }
        }
    }
}
