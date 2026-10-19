// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! One causal authority domain, with version-exact tombstone acknowledgements and v2 generations.

use super::*;
use crate::replica::version_hash;

impl<K, V, A> SignedReplica<K, V, A>
where
    K: Key + Hash,
    V: Value,
    A: Clone + Eq,
{
    /// Insert an authoritative local LWW value; `reconcile` propagates it asynchronously.
    ///
    /// # Errors
    /// A read-only node cannot write, or this key/value cannot fit bounded signed framing.
    pub fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "signed value-only reader cannot originate dated writes",
            ));
        }
        let entry = Entry::present(self.clock.now(), value);
        self.ensure_entry_fits(&key, &entry)?;
        self.record_entry(key, entry);
        Ok(())
    }

    /// Create a local timestamped tombstone (also for an absent key); it propagates on reconcile.
    ///
    /// # Errors
    /// A read-only node cannot delete, or the encoded tombstone is beyond framing limits.
    pub fn remove(&mut self, key: K) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "signed value-only reader cannot originate deletions",
            ));
        }
        let entry = Entry::tombstone(self.clock.now());
        self.ensure_entry_fits(&key, &entry)?;
        self.record_entry(key, entry);
        Ok(())
    }

    fn record_entry(&mut self, key: K, entry: Entry<Timestamp, V>) {
        let mut generation = self.generations.mutation();
        if self.tombstone_acks.remove(&key).is_some() {
            generation.record_ack_key_clear(key.clone());
        }
        generation.record_entry(key.clone(), entry.clone());
        self.entries.insert(key, entry);
    }

    pub(super) fn apply_remote_entry(
        &mut self,
        peer: VerifiedPeerId,
        key: K,
        remote: Entry<Timestamp, V>,
    ) -> Option<u64> {
        if !self.authorities.contains(&peer.peer_id()) {
            return None;
        }
        self.clock.observe(remote.stamp);
        let merged = match self.entries.get(&key) {
            Some(local) => local.merge(&remote),
            None => remote,
        };
        let changed = self
            .entries
            .get(&key)
            .is_none_or(|local| rsos::digest(local) != rsos::digest(&merged));
        if changed {
            self.record_entry(key.clone(), merged);
        }
        self.entries
            .get(&key)
            .filter(|entry| entry.is_tombstone())
            .map(version_hash)
    }

    pub(super) fn accept_ack(&mut self, peer: VerifiedPeerId, key: K, version: u64) {
        let id = peer.peer_id();
        if !self.members.contains(&id) {
            return;
        }
        let Some(entry) = self.entries.get(&key) else {
            return;
        };
        if !entry.is_tombstone() || version_hash(entry) != version {
            return;
        }
        let acks = self.tombstone_acks.entry(key.clone()).or_default();
        if acks.insert(id, version) != Some(version) {
            self.generations
                .mutation()
                .record_ack(key, id, Some(version));
        }
    }

    /// Inspect the last accepted exact-version ACK for an authoritative peer.
    pub fn acknowledged_version(&self, key: &K, peer: PeerId) -> Option<u64> {
        self.tombstone_acks
            .get(key)
            .and_then(|acks| acks.get(&peer).copied())
    }

    /// Snapshot the causal ACK matrix, keyed exclusively by verified principal identities.
    pub fn tombstone_ack_snapshot(&self) -> HashMap<K, HashMap<PeerId, u64>> {
        self.tombstone_acks.clone()
    }

    /// True only when every provisioned authoritative member acknowledged this exact tombstone.
    ///
    /// The configured roster is installed as blockers at construction, including offline peers.
    /// Operators must still enforce their tombstone retention period before collection.
    pub fn is_tombstone_stable(&self, key: &K) -> bool {
        let Some(entry) = self.entries.get(key) else {
            return false;
        };
        if !entry.is_tombstone() {
            return false;
        }
        let version = version_hash(entry);
        self.members
            .iter()
            .all(|id| self.tombstone_acks.get(key).and_then(|acks| acks.get(id)) == Some(&version))
    }

    /// Collect a tombstone only if every authoritative member has ACKed its exact version.
    ///
    /// This is explicitly invoked by the operator after its retention policy has elapsed; a
    /// reader cannot collect tombstones. Unacknowledged members are never silently evicted.
    pub fn collect_if_stable(&mut self, key: &K) -> bool {
        if self.read_only || !self.is_tombstone_stable(key) {
            return false;
        }
        let mut generation = self.generations.mutation();
        self.entries.remove(key);
        generation.record_physical_delete(key.clone());
        if self.tombstone_acks.remove(key).is_some() {
            generation.record_ack_key_clear(key.clone());
        }
        true
    }

    /// Explicitly retire one logical peer and all of its outstanding ACK obligations.
    ///
    /// Authorization is also removed from this runtime's authoritative roster. A coordinated
    /// deployment must revoke its provisioned signer on every node before the next restart;
    /// offline resets of one replica's causal history are not safe.
    pub fn decommission(&mut self, peer: PeerId) -> bool {
        self.routes.decommission(peer);
        self.outbound_cursors.remove(&peer);
        self.readers.remove(&peer);
        self.authorities.remove(&peer);
        let removed = self.members.remove(&peer);
        let mut generation = self.generations.mutation();
        if removed {
            generation.record_member(peer, false);
        }
        for (key, acks) in &mut self.tombstone_acks {
            if acks.remove(&peer).is_some() {
                generation.record_ack(key.clone(), peer, None);
            }
        }
        removed
    }

    /// Atomically save the signed identity schema v2 with incremental generation semantics.
    ///
    /// A failed commit leaves its frozen generation retryable; concurrent writes to this
    /// single-writer actor cannot interleave within this method.
    ///
    /// # Errors
    /// Propagates backend errors without retiring a failed generation.
    pub fn snapshot_now(&mut self) -> io::Result<()> {
        let frozen = self.generations.freeze();
        let delta = frozen
            .as_ref()
            .map(|generation| generation.persistence_delta());
        if !self.persistence.try_save_delta(delta.as_ref())? {
            self.persistence.save(&PersistedState::new(
                self.entries
                    .iter()
                    .map(|(key, entry)| (key.clone(), entry.clone()))
                    .collect(),
                self.members.clone(),
                self.tombstone_acks.clone(),
            ))?;
        }
        if let Some(frozen) = frozen {
            assert!(
                self.generations.commit(frozen.id),
                "the signed single-writer snapshot has no concurrent generation commit"
            );
        }
        Ok(())
    }
}
