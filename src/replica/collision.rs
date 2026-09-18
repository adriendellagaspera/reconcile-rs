// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Node-id collision detection.
//!
//! A random 64-bit `NodeId` is unique only probabilistically
//! ([`NodeId`](crate::clock::NodeId)'s docs carry the bound). If two nodes share an id and write
//! the same key at the same HLC instant, both writes carry the identical [`Timestamp`] while
//! holding different content. LWW can no longer order them, so each side could otherwise retain
//! its own value indefinitely.
//!
//! The exact observable signature is therefore equal stamps with unequal content, checked at the
//! merge where it matters. Re-delivery of the same write has equal stamp *and* equal content and
//! does not trip the detector; the content digest is computed only after the stamps compare equal.

use std::sync::atomic::{AtomicBool, Ordering};

use tracing::error;

use crate::bounds::Value;
use crate::clock::{NodeId, Timestamp};
use crate::entry::Entry;

/// `true` when `local` and `remote` claim the same [`Timestamp`] while holding different content
/// -- the observable signature of two nodes sharing a [`NodeId`](crate::clock::NodeId).
pub(crate) fn is_node_id_collision<V: Value>(
    local: &Entry<Timestamp, V>,
    remote: &Entry<Timestamp, V>,
) -> bool {
    local.stamp == remote.stamp && rsos::digest(&local.state) != rsos::digest(&remote.state)
}

/// The once-per-replica guard around the collision report.
///
/// A collision is not a single event: while one lasts, every key of every round trips the
/// detector. A per-key line would bury the one fact an operator needs, so the first call reports
/// and every later one is silent.
pub(crate) struct CollisionReporter(AtomicBool);

impl CollisionReporter {
    pub(crate) fn new() -> Self {
        CollisionReporter(AtomicBool::new(false))
    }

    /// Report a detected collision, returning whether *this* call is the one that reported.
    ///
    /// `error!`, not `warn!`: unlike the ephemeral-identity warning on
    /// [`with_persistence`](crate::ReplicatedMap::with_persistence), this is not a configuration
    /// smell that may be deliberate -- it is data not converging, now.
    pub(crate) fn report(&self, node_id: NodeId) -> bool {
        if self.0.swap(true, Ordering::Relaxed) {
            return false;
        }
        error!(
            node_id = node_id.get(),
            "two peers are writing under the same node id: a remote entry carries this node's \
             exact timestamp with different content, which last-write-wins cannot order, so the \
             two sides will not converge on the affected keys. Node ids are drawn at random and \
             are unique only probabilistically (lww_register::clock::NodeId); set a distinct, \
             stable Config::with_node_id on every node. Reported once per process."
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime};

    fn stamp(ms: u64, node: u64) -> Timestamp {
        Timestamp::new(
            Hlc::new(PhysicalTime::from_millis(ms), LogicalCounter::new(0)),
            NodeId::new(node),
        )
    }

    /// The fault itself: one stamp, two contents. This is what a shared node id produces, and
    /// what `Entry::merge` then resolves differently on each side.
    #[test]
    fn equal_stamps_with_different_values_are_a_collision() {
        let s = stamp(100, 7);
        assert!(is_node_id_collision(
            &Entry::present(s, "ours".to_string()),
            &Entry::present(s, "theirs".to_string())
        ));
    }

    /// A tombstone and a live value at one stamp is the same fault, not a special case.
    #[test]
    fn equal_stamps_with_a_tombstone_on_one_side_are_a_collision() {
        let s = stamp(100, 7);
        assert!(is_node_id_collision(
            &Entry::present(s, "ours".to_string()),
            &Entry::<Timestamp, String>::tombstone(s)
        ));
    }

    /// Our own write coming back from a peer: same stamp, same content. Reporting this would make
    /// the warning fire on every ordinary reconciliation round.
    #[test]
    fn an_echo_of_our_own_write_is_not_a_collision() {
        let s = stamp(100, 7);
        assert!(!is_node_id_collision(
            &Entry::present(s, "same".to_string()),
            &Entry::present(s, "same".to_string())
        ));
    }

    /// The guard's whole contract: the first call reports, every later one is silent. Without
    /// this, a reporter that never reports -- or one that reports on every key of every round --
    /// both pass the detector tests above.
    #[test]
    fn the_first_report_is_the_only_one() {
        let reporter = CollisionReporter::new();
        assert!(
            reporter.report(NodeId::new(7)),
            "the first call must report"
        );
        assert!(
            !reporter.report(NodeId::new(7)),
            "the second call must stay quiet"
        );
        assert!(
            !reporter.report(NodeId::new(7)),
            "and so must every later one"
        );
    }

    /// Different stamps are ordinary LWW, whatever the contents: `merge` has a total order to
    /// work with and both sides converge on the same winner.
    #[test]
    fn different_stamps_are_never_a_collision() {
        assert!(!is_node_id_collision(
            &Entry::present(stamp(100, 7), "ours".to_string()),
            &Entry::present(stamp(200, 7), "theirs".to_string())
        ));
        assert!(!is_node_id_collision(
            &Entry::present(stamp(100, 7), "ours".to_string()),
            &Entry::present(stamp(100, 8), "theirs".to_string())
        ));
    }
}
