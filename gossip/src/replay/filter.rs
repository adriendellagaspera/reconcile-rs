// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`ReplayFilter`]'s per-peer map, staleness purge and public entry points.
//!
//! The staleness purge (module docs' "Replay state outlives membership" rule) never runs on
//! decommission — only opportunistically, keyed on a peer's own `stamp_at_max`, on every
//! `check_and_record` call. A decommissioned-but-not-stale peer's entry is never touched here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use parking_lot::Mutex;

use super::peer_state::PeerState;
use super::{ReplayFilter, Seq, Stamp};

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
        if !stamp.is_fresh(now, self.freshness_window) {
            return false;
        }

        let mut map = self.peers.lock();

        // Opportunistic staleness purge; sound because a replayable stamp would fail above.
        let window = self.freshness_window;
        map.retain(|_, s| s.stamp_at_max().age_relative_to(now) <= window.as_millis() as u64);

        match map.get_mut(&sender) {
            Some(state) => state.accept(seq, stamp),
            None if map.len() >= self.max_senders => false,
            None => {
                map.insert(sender, PeerState::new(seq, stamp));
                true
            }
        }
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
