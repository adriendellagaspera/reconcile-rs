// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Node-id collision detection (#24).
//!
//! A random 64-bit `NodeId` is unique only probabilistically
//! ([`NodeId`](crate::clock::NodeId)'s docs carry the bound). When two nodes do draw the same id
//! and write the same key in the same clock tick, both writes carry the *identical*
//! [`Timestamp`], and [`Entry::merge`] -- `max` over what is then no longer a total order --
//! keeps each side's own value on each side. The two never converge, and each keeps re-offering
//! its version forever: a silent divergence livelock.
//!
//! What is detected here is that exact state, at the merge where it first becomes observable,
//! rather than a proxy for it at startup. An announce-and-listen probe was the shape #24
//! sketched; it needs a sender identity on the wire, which this protocol has no field for -- the
//! only additive slots are the two tags #463 reserved, and spending one of two on a diagnostic
//! buys a *heuristic* ("nobody answered in 200 ms") where this buys the fault itself.
//!
//! Equal stamps with unequal content is the collision and essentially nothing else: a peer
//! echoing back a write of ours carries the same stamp *and* the same content, so it does not
//! trip. The digest is computed only once stamps compare equal, which for distinct ids requires
//! the same node, same physical millisecond and same counter -- i.e. effectively never.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use tracing::error;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::Entry;

use super::Replica;

/// `true` when `local` and `remote` claim the same [`Timestamp`] while holding different content
/// -- the observable signature of two nodes sharing a [`NodeId`](crate::clock::NodeId).
pub(crate) fn is_node_id_collision<V: Value>(
    local: &Entry<Timestamp, V>,
    remote: &Entry<Timestamp, V>,
) -> bool {
    local.stamp == remote.stamp && rsos::digest(&local.state) != rsos::digest(&remote.state)
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Report a detected collision, once per replica: while one lasts, every key of every round
    /// trips the same detector, and a per-key line would bury the one fact an operator needs.
    ///
    /// `error!`, not `warn!`: unlike the ephemeral-identity warning on
    /// [`with_persistence`](crate::ReplicatedMap::with_persistence), this is not a configuration
    /// smell that may be deliberate -- it is data not converging, now.
    pub(crate) fn report_node_id_collision(&self) {
        if self.collision_reported.swap(true, Ordering::Relaxed) {
            return;
        }
        error!(
            node_id = self.node_id().get(),
            "two peers are writing under the same node id: a remote entry carries this node's \
             exact timestamp with different content, which last-write-wins cannot order, so the \
             two sides will not converge on the affected keys. Node ids are drawn at random and \
             are unique only probabilistically (lww_register::clock::NodeId); set a distinct, \
             stable Config::with_node_id on every node. Reported once per process."
        );
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
