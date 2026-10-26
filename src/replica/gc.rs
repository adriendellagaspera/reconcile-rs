// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use serde::Serialize;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::Entry;

use super::Replica;

/// A deterministic, cross-node version token for a value: the low 64 bits of `rsos::digest`
/// .
/// A peer acknowledges the exact tombstone version it holds, so a stale ack cannot authorize GC of
/// a newer one.
pub(crate) fn version_hash<V: Serialize>(value: &V) -> u64 {
    rsos::digest(value).0[0]
}

/// Identity-agnostic causal GC predicate. Membership identities must have been admitted by the
/// owning runtime's authentication policy; the predicate never infers identities from routes.
fn all_members_acknowledged<K: Eq + Hash, P: Eq + Hash>(
    members: &HashSet<P>,
    tombstone_acks: &HashMap<K, HashMap<P, u64>>,
    key: &K,
    version: u64,
) -> bool {
    if members.is_empty() {
        return true;
    }
    let Some(key_acks) = tombstone_acks.get(key) else {
        return false;
    };
    members
        .iter()
        .all(|peer| key_acks.get(peer) == Some(&version))
}

/// Receipt ACKs do not fence rollback or delayed application data. Keep physical collection
/// closed until the runtime can prove obsolete history is no longer admissible.
pub(crate) fn has_collection_proof() -> bool {
    false
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Remove a key from the dated `map`, its value-only projection, and the live-tombstone
    /// index (the GC removal path).
    #[cfg(test)]
    pub(crate) fn gc_remove(&self, key: &K) -> Option<Entry<Timestamp, V>> {
        let mut generation = self.snapshot_generations.mutation();
        let _guard = self.write_lock.lock();
        let mut map = (*self.map.load_full()).clone();
        let mut projection = (*self.projection.load_full()).clone();
        self.live_tombstones.write().remove(key);
        projection.remove(key);
        let ret = map.remove(key);
        self.map.store(Arc::new(map));
        self.projection.store(Arc::new(projection));
        if ret.is_some() {
            generation.record_physical_delete(key.clone());
            self.record_changes(1);
        }
        ret
    }

    /// The public runtime path retains tombstones until distributed history is fenced.
    pub(crate) fn gc_collect(
        &self,
        key: &K,
        expected: Timestamp,
        now: DateTime<Utc>,
        retention: impl FnOnce() -> Duration,
    ) -> Option<Entry<Timestamp, V>> {
        if !has_collection_proof() {
            return None;
        }
        self.collect_candidate(key, expected, now, retention)
    }

    /// Conditional local transaction. This does not itself establish a distributed history proof;
    /// only the gated runtime path may call it outside the local-invariant tests.
    pub(super) fn collect_candidate(
        &self,
        key: &K,
        expected: Timestamp,
        now: DateTime<Utc>,
        retention: impl FnOnce() -> Duration,
    ) -> Option<Entry<Timestamp, V>> {
        let mut generation = self.snapshot_generations.mutation();
        let _guard = self.write_lock.lock();
        let root = self.map.load_full();
        let entry = root.get(key)?;
        if !entry.is_tombstone() || entry.stamp != expected {
            return None;
        }
        let now_ms: u64 = now.timestamp_millis().try_into().ok()?;
        let expires = u128::from(entry.stamp.physical().millis()) + retention().as_millis();
        if expires >= u128::from(now_ms) || !self.is_tombstone_stable(key, version_hash(entry)) {
            return None;
        }
        let mut map = (*root).clone();
        let mut projection = (*self.projection.load_full()).clone();
        self.live_tombstones.write().remove(key);
        projection.remove(key);
        let ret = map.remove(key);
        self.map.store(Arc::new(map));
        self.projection.store(Arc::new(projection));
        let mut changed = 0;
        if ret.is_some() {
            generation.record_physical_delete(key.clone());
            changed += 1;
        }
        if self.tombstone_acks.write().remove(key).is_some() {
            generation.record_ack_key_clear(key.clone());
            changed += 1;
        }
        self.record_changes(changed);
        ret
    }

    /// Whether every current member has acknowledged this version. The empty receipt quorum
    /// is complete; neither case establishes that obsolete application history is fenced.
    pub(crate) fn is_tombstone_stable(&self, key: &K, version: u64) -> bool {
        all_members_acknowledged(
            &self.members.read(),
            &self.tombstone_acks.read(),
            key,
            version,
        )
    }

    /// Drop the acknowledgment bookkeeping for a key once its tombstone has been collected.
    #[cfg(test)]
    pub(crate) fn forget_tombstone(&self, key: &K) {
        let mut generation = self.snapshot_generations.mutation();
        if self.tombstone_acks.write().remove(key).is_some() {
            generation.record_ack_key_clear(key.clone());
            self.record_changes(1);
        }
    }

    /// Whether `peer` still owes an acknowledgment on some held tombstone — i.e. whether its
    /// absence would block GC.
    /// Walks [`live_tombstones`](Self::live_tombstones), not
    /// [`tombstone_acks`](Self::tombstone_acks): a freshly deleted tombstone has no ack entry yet
    /// and must still count as pending.
    pub(crate) fn has_pending_tombstone_acks(&self, peer: IpAddr) -> bool {
        let live = self.live_tombstones.read();
        if live.is_empty() {
            return false;
        }
        let map = self.map.load_full();
        let acks = self.tombstone_acks.read();
        live.iter().any(|key| {
            let Some(entry) = map.get(key) else {
                return false;
            };
            let version = version_hash(entry);
            acks.get(key).and_then(|peer_acks| peer_acks.get(&peer)) != Some(&version)
        })
    }
}

#[cfg(test)]
mod tests;
