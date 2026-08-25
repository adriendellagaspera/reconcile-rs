// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::value_ref::{Snapshot, ValueRef};
use crate::FingerprintTreeMap;
use rsos::Fingerprint;

use super::ReplicatedMap;

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// A zero-copy `Arc` snapshot of the dated map as it stands right now (#34): `rsos`'s
    /// `iter`/`range` borrow straight from it, with no lock held and no lifetime tied back to
    /// `self` — a concurrent write on this handle installs a fresh tree behind a new `Arc` and
    /// leaves this snapshot untouched.
    ///
    /// Entries are the raw `Entry<Timestamp, V>` wire representation, tombstones included —
    /// unlike [`to_vec`](Self::to_vec)/[`for_each`](Self::for_each), this does not filter them or
    /// unwrap the value; a caller wanting only live values checks
    /// [`is_tombstone`](crate::entry::Entry::is_tombstone)/[`value`](crate::entry::Entry::value)
    /// itself.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// use reconcile::{replicated_map::Config, InMemoryNetwork, ReplicatedMap};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let network = InMemoryNetwork::new();
    /// let transport = Arc::new(network.bind("127.0.0.1:8308".parse().unwrap()));
    /// let store = ReplicatedMap::<String, i32>::new_with_transport(
    ///     Config::default().with_insecure_no_key(),
    ///     transport,
    /// );
    /// store.insert("a".to_string(), 1);
    ///
    /// let snapshot = store.snapshot();
    /// let live: Vec<_> = snapshot.range(..).filter(|(_, e)| !e.is_tombstone()).collect();
    /// assert_eq!(live.len(), 1);
    /// # }
    /// ```
    pub fn snapshot(&self) -> Arc<FingerprintTreeMap<K, Entry<Timestamp, V>>> {
        self.engine.map.load_full()
    }

    /// As [`snapshot`](Self::snapshot), but of the timestamp-less value-only projection — the
    /// same tree [`value_fingerprint`](Self::value_fingerprint) measures and a converged
    /// [`ReadReplicaMap`](crate::read_replica_map::ReadReplicaMap) mirrors.
    pub fn value_snapshot(&self) -> Arc<FingerprintTreeMap<K, State<V>>> {
        self.engine.projection.load_full()
    }

    /// Fingerprint of the live entries (value **and** timestamp) over `range`: `O(range size)`,
    /// used as the anti-entropy comparison value — equal fingerprints on both peers mean equal
    /// content over the range. See [`value_fingerprint`](Self::value_fingerprint) for the
    /// timestamp-less counterpart.
    pub fn fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.engine.fingerprint(range)
    }

    /// Fingerprint of the **value-only projection** over a range: the timestamp-less counterpart
    /// of [`fingerprint`](Self::fingerprint), which a converged
    /// [`ReadReplicaMap`](crate::read_replica_map::ReadReplicaMap) reproduces.
    pub fn value_fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.engine.value_fingerprint(range)
    }

    /// Unlike before #34, holding the returned [`ValueRef`] does **not** block a concurrent write
    /// on the same handle — it owns an immutable snapshot of the tree as it stood when `get`
    /// returned, not a lock. [`get_cloned`](Self::get_cloned) remains the default read when the
    /// value will be compared against or fed into a subsequent write and a clone is cheap enough;
    /// [`update`](Self::update) is still the one that makes that read-then-write atomic.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// use reconcile::{replicated_map::Config, InMemoryNetwork, ReplicatedMap};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let network = InMemoryNetwork::new();
    /// let transport = Arc::new(network.bind("127.0.0.1:8302".parse().unwrap()));
    /// let store = ReplicatedMap::<String, i32>::new_with_transport(
    ///     Config::default().with_insecure_no_key(),
    ///     transport,
    /// );
    ///
    /// assert!(store.get(&"a".to_string()).is_none());
    /// store.insert("a".to_string(), 1);
    /// assert_eq!(store.get(&"a".to_string()).as_deref(), Some(&1)); // ValueRef derefs to &V
    /// # }
    /// ```
    pub fn get(&self, k: &K) -> Option<ValueRef<K, V>> {
        let snapshot = self.engine.map.load_full();
        snapshot.get(k)?.value()?;
        Some(ValueRef(Snapshot::Dated(snapshot, k.clone())))
    }

    /// Clone of the live value for `k`, or `None`. Cheaper than holding a [`ValueRef`] when the
    /// value itself, not a reference into the snapshot, is what a subsequent write needs. Still
    /// racy against a concurrent write between the read and the write; use [`update`](Self::update)
    /// instead when the write must be atomic with the read.
    pub fn get_cloned(&self, k: &K) -> Option<V> {
        self.get(k).map(|v| v.clone())
    }

    /// The number of **live** entries. `O(n)`, and smaller than the raw map size: tombstones
    /// linger until causal-stability-gated GC reclaims them.
    pub fn len(&self) -> usize {
        self.engine
            .map
            .load_full()
            .iter()
            .filter(|(_, entry)| !entry.is_tombstone())
            .count()
    }

    /// Whether the store holds no live entry. `O(n)` worst case, but returns as soon as it finds a
    /// live value. A store that holds only tombstones is empty.
    pub fn is_empty(&self) -> bool {
        !self
            .engine
            .map
            .load_full()
            .iter()
            .any(|(_, entry)| !entry.is_tombstone())
    }

    /// Whether `k` maps to a live value (a tombstoned key reads as absent).
    pub fn contains_key(&self, k: &K) -> bool {
        self.get(k).is_some()
    }

    /// The smallest live key and its value, or `None` if the store holds no live entry. `O(log n)`,
    /// worse if the smallest raw key is tombstoned (`O(n)` if every entry is).
    pub fn first_key_value(&self) -> Option<(K, V)> {
        let guard = self.engine.map.load_full();
        guard
            .iter()
            .find(|(_, entry)| !entry.is_tombstone())
            .map(|(k, entry)| (k.clone(), entry.value().expect("checked above").clone()))
    }

    /// The largest live key and its value, or `None` if the store holds no live entry. Same
    /// complexity as [`first_key_value`](Self::first_key_value).
    pub fn last_key_value(&self) -> Option<(K, V)> {
        let guard = self.engine.map.load_full();
        let mut index = guard.len();
        while index > 0 {
            index -= 1;
            let key = guard.select(index).clone();
            if let Some(value) = guard.get(&key).and_then(|entry| entry.value()) {
                return Some((key, value.clone()));
            }
        }
        None
    }

    /// Call `f` for every live entry, in key order, over a snapshot of the map as it stood when
    /// this call started — a concurrent write on the same handle is invisible to it and does not
    /// block on it either way.
    pub fn for_each<F: FnMut(&K, &V)>(&self, mut f: F) {
        let guard = self.engine.map.load_full();
        for (k, entry) in guard.iter() {
            if let Some(value) = entry.value() {
                f(k, value);
            }
        }
    }

    /// Call `f` for every live entry whose key falls in `range`, in key order. Mirrors the
    /// [`fingerprint`](Self::fingerprint) range signature; same snapshot discipline as
    /// [`for_each`](Self::for_each).
    pub fn for_each_in_range<R: RangeBounds<K>, F: FnMut(&K, &V)>(&self, range: R, mut f: F) {
        let guard = self.engine.map.load_full();
        for (k, entry) in guard.range(range) {
            if let Some(value) = entry.value() {
                f(k, value);
            }
        }
    }

    /// Snapshot all live entries into an owned `Vec`, in key order. Clones every value; prefer
    /// [`for_each`](Self::for_each) to avoid the copy for large scans.
    pub fn to_vec(&self) -> Vec<(K, V)> {
        let guard = self.engine.map.load_full();
        guard
            .iter()
            .filter_map(|(k, entry)| entry.value().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    /// Snapshot the live entries whose keys fall in `range` into an owned `Vec`, in key order.
    pub fn range_to_vec<R: RangeBounds<K>>(&self, range: R) -> Vec<(K, V)> {
        let guard = self.engine.map.load_full();
        guard
            .range(range)
            .filter_map(|(k, entry)| entry.value().map(|value| (k.clone(), value.clone())))
            .collect()
    }

    /// The keys of all live entries, in key order. Thin owned convenience over [`to_vec`](Self::to_vec).
    pub fn keys(&self) -> Vec<K> {
        let guard = self.engine.map.load_full();
        guard
            .iter()
            .filter_map(|(k, entry)| entry.value().map(|_| k.clone()))
            .collect()
    }

    /// The values of all live entries, in key order.
    pub fn values(&self) -> Vec<V> {
        let guard = self.engine.map.load_full();
        guard
            .iter()
            .filter_map(|(_, entry)| entry.value().cloned())
            .collect()
    }
}
