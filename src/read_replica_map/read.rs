// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::ops::RangeBounds;
use std::sync::Arc;

use crate::bounds::{Key, Value};
use crate::entry::State;
use crate::value_ref::{Snapshot, ValueRef};
use crate::FingerprintTreeMap;
use rsos::Fingerprint;

use super::ReadReplicaMap;

impl<K: Key, V: Value> ReadReplicaMap<K, V> {
    /// Get the live value for a key, or `None` if the key is absent or holds a replicated
    /// tombstone. Unlike before #34, the returned [`ValueRef`] owns an immutable snapshot rather
    /// than a lock, so holding it never blocks a concurrent inbound update from integrating.
    pub fn get(&self, k: &K) -> Option<ValueRef<K, V>> {
        let snapshot = self.tree.load_full();
        snapshot.get(k)?.as_value()?;
        Some(ValueRef(Snapshot::Projected(snapshot, k.clone())))
    }

    /// A zero-copy `Arc` snapshot of the value-only tree as it stands right now (#34): `rsos`'s
    /// `iter`/`range` borrow straight from it, with no lock held and no lifetime tied back to
    /// `self`. Entries are the raw [`State`] wire representation, tombstones included — a caller
    /// wanting only live values checks [`State::as_value`] itself.
    pub fn snapshot(&self) -> Arc<FingerprintTreeMap<K, State<V>>> {
        self.tree.load_full()
    }

    /// Clone of the live value for `k`, or `None`. Cheaper than holding a [`ValueRef`] snapshot
    /// when the value itself, not a reference into it, is what a subsequent write needs; mirrors
    /// [`ReplicatedMap::get_cloned`](crate::ReplicatedMap::get_cloned).
    pub fn get_cloned(&self, k: &K) -> Option<V> {
        self.get(k).map(|v| v.clone())
    }

    /// Whether the read replica currently holds a live value for the key (a tombstone counts as
    /// absent).
    pub fn contains_key(&self, k: &K) -> bool {
        self.tree
            .load_full()
            .get(k)
            .is_some_and(|state| !state.is_tombstone())
    }

    /// The number of **live** entries currently held (a replicated tombstone counts as absent, not
    /// present). Mirrors [`ReplicatedMap::len`](crate::ReplicatedMap::len): `O(n)`, it scans the
    /// tree filtering out tombstones.
    pub fn len(&self) -> usize {
        self.tree
            .load_full()
            .iter()
            .filter(|(_, state)| !state.is_tombstone())
            .count()
    }

    /// Whether the read replica holds no live entry (a tree holding only tombstones is empty).
    /// `O(n)` worst case, but returns as soon as it finds a live value.
    pub fn is_empty(&self) -> bool {
        !self
            .tree
            .load_full()
            .iter()
            .any(|(_, state)| !state.is_tombstone())
    }

    /// Value-only fingerprint over a range. After convergence this equals the dated peer's
    /// [`value_fingerprint`](crate::ReplicatedMap::value_fingerprint) over the same range.
    pub fn value_fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.tree.load_full().aggregate(range).fingerprint()
    }

    /// Deprecated alias for [`value_fingerprint`](Self::value_fingerprint) — the name collided
    /// with [`ReplicatedMap::fingerprint`](crate::ReplicatedMap::fingerprint), which includes the
    /// timestamp and so never equals this one between converged peers (#294).
    #[deprecated(since = "1.0.0", note = "renamed to `value_fingerprint`")]
    pub fn fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.value_fingerprint(range)
    }

    /// The smallest live key and its value, or `None` if the read replica holds no live entry.
    /// Same complexity as [`ReplicatedMap::first_key_value`](crate::ReplicatedMap::first_key_value).
    pub fn first_key_value(&self) -> Option<(K, V)> {
        let guard = self.tree.load_full();
        guard
            .iter()
            .find(|(_, state)| !state.is_tombstone())
            .map(|(k, state)| (k.clone(), state.as_value().expect("checked above").clone()))
    }

    /// The largest live key and its value, or `None` if the read replica holds no live entry. Same
    /// complexity as [`ReplicatedMap::last_key_value`](crate::ReplicatedMap::last_key_value).
    pub fn last_key_value(&self) -> Option<(K, V)> {
        let guard = self.tree.load_full();
        let mut index = guard.len();
        while index > 0 {
            index -= 1;
            let key = guard.select(index).clone();
            if let Some(value) = guard.get(&key).and_then(|state| state.as_value()) {
                return Some((key, value.clone()));
            }
        }
        None
    }

    /// Call `f` for every live entry, in key order, over a snapshot of the tree as it stood when
    /// this call started. Do not block or call back into the replica from `f`.
    pub fn for_each<F: FnMut(&K, &V)>(&self, mut f: F) {
        let guard = self.tree.load_full();
        for (k, state) in guard.iter() {
            if let Some(value) = state.as_value() {
                f(k, value);
            }
        }
    }

    /// Call `f` for every live entry whose key falls in `range`, in key order. Mirrors the
    /// [`value_fingerprint`](Self::value_fingerprint) range signature; same snapshot discipline as
    /// [`for_each`](Self::for_each).
    pub fn for_each_in_range<R: RangeBounds<K>, F: FnMut(&K, &V)>(&self, range: R, mut f: F) {
        let guard = self.tree.load_full();
        for (k, state) in guard.range(range) {
            if let Some(value) = state.as_value() {
                f(k, value);
            }
        }
    }

    /// Snapshot all live entries into an owned `Vec`, in key order. Clones every value; prefer
    /// [`for_each`](Self::for_each) to avoid the copy for large scans.
    pub fn to_vec(&self) -> Vec<(K, V)> {
        let guard = self.tree.load_full();
        guard
            .iter()
            .filter_map(|(k, state)| state.as_value().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    /// Snapshot the live entries whose keys fall in `range` into an owned `Vec`, in key order.
    pub fn range_to_vec<R: RangeBounds<K>>(&self, range: R) -> Vec<(K, V)> {
        let guard = self.tree.load_full();
        guard
            .range(range)
            .filter_map(|(k, state)| state.as_value().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    /// The keys of all live entries, in key order. Thin owned convenience over [`to_vec`](Self::to_vec).
    pub fn keys(&self) -> Vec<K> {
        let guard = self.tree.load_full();
        guard
            .iter()
            .filter_map(|(k, state)| state.as_value().map(|_| k.clone()))
            .collect()
    }

    /// The values of all live entries, in key order.
    pub fn values(&self) -> Vec<V> {
        let guard = self.tree.load_full();
        guard
            .iter()
            .filter_map(|(_, state)| state.as_value().cloned())
            .collect()
    }
}
