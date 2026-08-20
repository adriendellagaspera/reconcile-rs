// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deterministic counters for the work the RSOS contract mandates — the *counted* half of the
//! write-cost question `benches/contention.rs` can only put in wall-clock terms (#455, #457).
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
//! # Cost when disabled
//!
//! Off unless `internal-testing` is on. The call sites carry no `#[cfg]`: they call
//! `record_aggregate_update`, which is this module's live one or its empty-bodied one, chosen by
//! `cfg` here rather than at each call site.
//!
//! # Threading
//!
//! Counts are **per-thread** (`thread_local!` + [`Cell`](std::cell::Cell), never an atomic): a
//! shared counter would itself be a contention point, perturbing the very measurement it exists to
//! explain. Read them from a single-threaded pass — the count is deterministic, so one such pass
//! characterizes every writer count.

#[cfg(feature = "internal-testing")]
pub use enabled::{snapshot, Counts};

#[cfg(not(feature = "internal-testing"))]
pub(crate) use disabled::record_aggregate_update;
#[cfg(feature = "internal-testing")]
pub(crate) use enabled::record_aggregate_update;

/// The live half: what the crate compiles against when `internal-testing` is on.
#[cfg(feature = "internal-testing")]
pub mod enabled {
    use std::cell::Cell;
    use std::ops::Sub;

    thread_local! {
        static AGGREGATE_UPDATES: Cell<u64> = const { Cell::new(0) };
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
    }

    impl Sub for Counts {
        type Output = Counts;

        /// Componentwise, saturating: a difference taken against a later snapshot is a caller
        /// error, not a reason to panic in a build that only exists to be measured.
        fn sub(self, rhs: Counts) -> Counts {
            Counts {
                aggregate_updates: self.aggregate_updates.saturating_sub(rhs.aggregate_updates),
            }
        }
    }

    /// This thread's counters as they stand.
    #[must_use]
    pub fn snapshot() -> Counts {
        Counts {
            aggregate_updates: AGGREGATE_UPDATES.get(),
        }
    }

    /// Counts one write of a node's cached subtree aggregate.
    #[inline]
    pub(crate) fn record_aggregate_update() {
        AGGREGATE_UPDATES.set(AGGREGATE_UPDATES.get() + 1);
    }
}

/// The no-op half: what the crate compiles against when `internal-testing` is off.
#[cfg(not(feature = "internal-testing"))]
mod disabled {
    /// Records nothing — an empty body, so every call site vanishes.
    #[inline(always)]
    pub(crate) fn record_aggregate_update() {}
}
