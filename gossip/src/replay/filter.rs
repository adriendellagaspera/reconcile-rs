// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`ReplayFilter`]'s per-peer map, lazy staleness reclamation and public entry points.
//!
//! A known sender's staleness is checked against its own `stamp_at_max` on every datagram.
//! Unrelated stale entries are reclaimed only when a new sender encounters full capacity.
//! Replay state remains independent of peer decommissioning.

use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use std::time::Duration;

use parking_lot::Mutex;

use super::peer_state::PeerState;
use super::{ReplayFilter, Seq, Stamp, VerifiedPeerReplayFilter};
use crate::peer::identity::VerifiedPeerId;

impl ReplayFilter {
    /// Build an empty filter. `enabled` mirrors the owning
    /// [`crate::auth::Authenticator`]'s mode.
    pub fn new(freshness_window: Duration, enabled: bool) -> Self {
        ReplayFilter {
            peers: Mutex::new(HashMap::new()),
            freshness_window,
            enabled,
            max_senders: usize::MAX,
        }
    }

    /// Set the maximum number of distinct authenticated sender IPs whose replay state may be
    /// retained concurrently.
    ///
    /// At capacity, already-tracked senders continue through the normal replay checks. A new
    /// sender is rejected until an entry becomes older than the freshness window and is purged.
    /// Fresh entries are never evicted to make room, because doing so would reopen their replay
    /// window. Disabled filters allocate no sender state, so this limit has no effect on them.
    #[must_use]
    pub fn with_max_senders(mut self, max_senders: usize) -> Self {
        self.max_senders = max_senders;
        self
    }

    /// Whether a datagram from `sender` is fresh and unique; `false` means drop it silently.
    /// Always `true` on a disabled filter.
    pub fn check_and_record(&self, sender: IpAddr, seq: Seq, stamp: Stamp) -> bool {
        if !self.enabled {
            return true;
        }
        self.check_and_record_at(sender, seq, stamp, Stamp::now())
    }

    /// [`check_and_record`](Self::check_and_record) with an injectable `now`.
    pub(super) fn check_and_record_at(
        &self,
        sender: IpAddr,
        seq: Seq,
        stamp: Stamp,
        now: Stamp,
    ) -> bool {
        check_and_record_sender(
            &mut self.peers.lock(),
            sender,
            seq,
            stamp,
            now,
            self.freshness_window,
            self.max_senders,
        )
    }

    /// Number of peers currently tracked. For test assertions; the `reconcile_internal_testing` gate sits
    /// on `reconcile::testing::replay_filter_len`.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.peers.lock().len()
    }

    /// Drop a peer's replay state. For tests: production relies on the staleness purge.
    #[cfg(test)]
    pub fn evict(&self, peer: IpAddr) {
        self.peers.lock().remove(&peer);
    }
}

impl VerifiedPeerReplayFilter {
    /// Create an always-enabled replay filter for individually verified signers.
    pub fn new(freshness_window: Duration) -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
            freshness_window,
            max_senders: usize::MAX,
        }
    }

    /// Bound the number of distinct verified peer identities retained concurrently.
    ///
    /// Fresh sender state is never evicted to make room for another signer.
    #[must_use]
    pub fn with_max_senders(mut self, max_senders: usize) -> Self {
        self.max_senders = max_senders;
        self
    }

    /// Number of authenticated signer replay windows retained.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.peers.lock().len()
    }

    /// Internal gate: only the already-signature-verified envelope may supply this identity.
    pub(crate) fn check_and_record(&self, peer: VerifiedPeerId, seq: Seq, stamp: Stamp) -> bool {
        check_and_record_sender(
            &mut self.peers.lock(),
            peer,
            seq,
            stamp,
            Stamp::now(),
            self.freshness_window,
            self.max_senders,
        )
    }
}

// The legacy and signed filters deliberately share the same acceptance policy and bound.
fn check_and_record_sender<K: Eq + Hash>(
    map: &mut HashMap<K, PeerState>,
    sender: K,
    seq: Seq,
    stamp: Stamp,
    now: Stamp,
    freshness_window: Duration,
    max_senders: usize,
) -> bool {
    if !stamp.is_fresh(now, freshness_window) {
        return false;
    }

    let window_ms = freshness_window.as_millis() as u64;
    if let Some(state) = map.get_mut(&sender) {
        if state.stamp_at_max().age_relative_to(now) <= window_ms {
            return state.accept(seq, stamp);
        }
        // The expired sender's old replay window cannot authorize a new datagram.
        map.remove(&sender);
    }

    if map.len() >= max_senders {
        map.retain(|_, s| s.stamp_at_max().age_relative_to(now) <= window_ms);
        if map.len() >= max_senders {
            return false;
        }
    }

    map.insert(sender, PeerState::new(seq, stamp));
    true
}
