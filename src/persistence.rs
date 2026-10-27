// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Persistence state and ports owned by the replicated-map runtime.
//!
//! These types include causal-stability membership and tombstone acknowledgements, so they are
//! part of reconcile rather than the reusable LWW-register primitive. FileSnapshot is the
//! durable filesystem adapter; InMemoryPersistence is the non-durable default.

pub use crate::snapshot::FileSnapshot;

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::IpAddr;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::entry::Entry;

/// The store's keyed entries in their dated, tombstone-aware form.
pub type DatedEntries<K, V> = Vec<(K, Entry<Timestamp, V>)>;

/// Everything a replicated map needs to survive a restart without behaving like a fresh replica.
///
/// `P` is the causal-member identity: the default `IpAddr` preserves the current UDP runtime,
/// while a signed runtime can persist stable `PeerId`s without inventing synthetic IP routes.
/// Do not admit raw self-asserted `PeerId`s into the authoritative membership set; admission
/// requires verified, authorized transport identity. Disk compatibility is a separate contract.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize, P: Serialize",
    deserialize = "K: Deserialize<'de> + Eq + std::hash::Hash, V: Deserialize<'de>, P: Deserialize<'de> + Eq + std::hash::Hash"
))]
#[non_exhaustive]
pub struct PersistedState<K, V, P = IpAddr> {
    /// Every key with its dated value. A `None` payload is a tombstone.
    pub entries: DatedEntries<K, V>,
    /// Every peer this node has ever communicated with (causal-stability membership).
    pub members: HashSet<P>,
    /// Per-tombstone acknowledgments: `key -> (peer -> version token of the tombstone it holds)`.
    pub tombstone_acks: HashMap<K, HashMap<P, u64>>,
}

// Not `#[derive(Default)]`: a derive would add `K: Default, V: Default` bounds neither field
// actually needs (an empty `Vec`/`HashSet`/`HashMap` needs none of that on its element types).
impl<K, V, P> Default for PersistedState<K, V, P> {
    fn default() -> Self {
        PersistedState {
            entries: DatedEntries::default(),
            members: HashSet::default(),
            tombstone_acks: HashMap::default(),
        }
    }
}

impl<K, V, P> PersistedState<K, V, P> {
    /// Build a state from its three fields — `#[non_exhaustive]` blocks the struct-literal form
    /// outside this crate, even with every field named, so this is the constructor callers reach
    /// for instead.
    #[must_use]
    pub fn new(
        entries: DatedEntries<K, V>,
        members: HashSet<P>,
        tombstone_acks: HashMap<K, HashMap<P, u64>>,
    ) -> Self {
        PersistedState {
            entries,
            members,
            tombstone_acks,
        }
    }
}

/// Entries alone, with `members` and `tombstone_acks` empty — the common case in tests and any
/// fresh construction where causal-stability membership and tombstone acks haven't been observed
/// yet. Reach for [`PersistedState::new`] instead when either needs a real value.
impl<K, V, P> From<DatedEntries<K, V>> for PersistedState<K, V, P> {
    fn from(entries: DatedEntries<K, V>) -> Self {
        PersistedState {
            entries,
            ..Default::default()
        }
    }
}

/// Coalesced durable effects captured at one snapshot-generation boundary.
///
/// Hidden from the ordinary persistence API surface: generic backends can ignore it and keep using
/// full-state save/load. FileSnapshot consumes it to append an incremental segment.
#[doc(hidden)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize, P: Serialize",
    deserialize = "K: Deserialize<'de> + Eq + std::hash::Hash, V: Deserialize<'de>, P: Deserialize<'de> + Eq + std::hash::Hash"
))]
pub struct PersistenceDelta<K, V, P = IpAddr> {
    pub(crate) entries: HashMap<K, Option<Entry<Timestamp, V>>>,
    pub(crate) members: HashMap<P, bool>,
    pub(crate) ack_key_clears: HashSet<K>,
    pub(crate) ack_peers: HashMap<K, HashMap<P, Option<u64>>>,
}

impl<K, V, P> PersistenceDelta<K, V, P> {
    pub(crate) fn new(
        entries: HashMap<K, Option<Entry<Timestamp, V>>>,
        members: HashMap<P, bool>,
        ack_key_clears: HashSet<K>,
        ack_peers: HashMap<K, HashMap<P, Option<u64>>>,
    ) -> Self {
        Self {
            entries,
            members,
            ack_key_clears,
            ack_peers,
        }
    }
}

/// A pluggable durable backend for a replicated map.
/// Held behind an [`Arc`](std::sync::Arc) and snapshotted from a background task, hence
/// `Send + Sync + 'static`.
pub trait Persistence<K, V, P = IpAddr>: Send + Sync + 'static {
    /// Load the previously saved state, or `Ok(None)` if nothing was ever saved.
    /// # Call context
    /// Called synchronously and inline, once, from `reconcile::ReplicatedMap::with_persistence` —
    /// before the node starts gossiping, typically from within an already-`async` caller (see that
    /// method's own doctest). A slow or blocking implementation stalls whichever thread runs that
    /// call; nothing else is running yet for it to hold up. `O(state size)` — the trait offers no
    /// incremental load, so a large snapshot is read and deserialized in full, every time.
    fn load(&self) -> io::Result<Option<PersistedState<K, V, P>>>;
    /// Durably save the given state, atomically replacing any previous snapshot.
    /// # Call context
    /// Called synchronously and inline — never via `spawn_blocking`. The default generation hook
    /// delegates here with the complete state, so existing backends keep their historical
    /// `O(state size)` behavior unchanged. `state` is one coherent cut. The runtime retires only
    /// its frozen journal generation: newer writes remain pending, even if a later full fallback
    /// cut already includes them. Payload cloning and I/O do not hold the mutation boundary.
    fn save(&self, state: &PersistedState<K, V, P>) -> io::Result<()>;

    /// Try to durably commit a coherent generation from its coalesced delta without requiring a
    /// materialized full state. Return `Ok(true)` only when the generation is durably handled;
    /// `Ok(false)` asks the runtime to collect the complete state and call [`save`](Self::save).
    ///
    /// The default preserves historical behavior for every existing backend by always requesting
    /// the full-state fallback.
    #[doc(hidden)]
    fn try_save_delta(&self, delta: Option<&PersistenceDelta<K, V, P>>) -> io::Result<bool> {
        let _ = delta;
        Ok(false)
    }
}

/// The **default** backend: the latest snapshot in RAM, so **a restart loses everything**. Use
/// `reconcile::FileSnapshot` when a restart must recover.
pub struct InMemoryPersistence<K, V, P = IpAddr> {
    state: Mutex<Option<PersistedState<K, V, P>>>,
}

impl<K, V, P> Default for InMemoryPersistence<K, V, P> {
    fn default() -> Self {
        InMemoryPersistence {
            state: Mutex::new(None),
        }
    }
}

impl<K, V, P> InMemoryPersistence<K, V, P> {
    /// Create an empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V, P> Persistence<K, V, P> for InMemoryPersistence<K, V, P>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    P: Clone + Send + Sync + 'static,
{
    /// Returns the last saved state, or `Ok(None)`.
    /// # Panics
    /// If the internal mutex is poisoned.
    fn load(&self) -> io::Result<Option<PersistedState<K, V, P>>> {
        Ok(self.state.lock().unwrap().clone())
    }

    /// Replaces the in-memory snapshot with `state`.
    /// # Panics
    /// If the internal mutex is poisoned.
    fn save(&self, state: &PersistedState<K, V, P>) -> io::Result<()> {
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime};

    /// A representative snapshot: a live entry, a tombstone, two members, one ack.
    fn sample_state() -> PersistedState<i32, String> {
        let mut members = HashSet::new();
        members.insert("127.0.0.1".parse().unwrap());
        members.insert("127.0.0.2".parse().unwrap());

        let mut acks = HashMap::new();
        let mut key_acks = HashMap::new();
        key_acks.insert("127.0.0.1".parse().unwrap(), 42u64);
        acks.insert(7, key_acks);

        PersistedState {
            entries: vec![
                (
                    1,
                    Entry::present(
                        Timestamp::new(
                            Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::new(0)),
                            NodeId::new(7),
                        ),
                        "alive".to_string(),
                    ),
                ),
                (
                    2,
                    Entry::tombstone(Timestamp::new(
                        Hlc::new(PhysicalTime::from_millis(2_000), LogicalCounter::new(1)),
                        NodeId::new(7),
                    )),
                ),
            ],
            members,
            tombstone_acks: acks,
        }
    }

    fn assert_states_eq(a: &PersistedState<i32, String>, b: &PersistedState<i32, String>) {
        assert_eq!(a.entries, b.entries);
        assert_eq!(a.members, b.members);
        assert_eq!(a.tombstone_acks, b.tombstone_acks);
    }

    #[test]
    fn default_delta_hook_requests_full_state_fallback() {
        let backend = InMemoryPersistence::<i32, String>::new();
        assert!(
            !backend.try_save_delta(None).unwrap(),
            "generic persistence backends must request the full-state fallback"
        );
    }

    #[test]
    fn in_memory_roundtrips_within_process() {
        let backend = InMemoryPersistence::<i32, String>::new();
        assert!(backend.load().unwrap().is_none());

        let state = sample_state();
        backend.save(&state).unwrap();
        assert_states_eq(&backend.load().unwrap().unwrap(), &state);
    }

    #[test]
    fn in_memory_save_replaces_previous() {
        let backend = InMemoryPersistence::<i32, String>::new();

        let mut first = sample_state();
        first.entries = vec![(
            1,
            Entry::present(
                Timestamp::new(
                    Hlc::new(PhysicalTime::from_millis(1), LogicalCounter::new(0)),
                    NodeId::new(0),
                ),
                "first".to_string(),
            ),
        )];
        backend.save(&first).unwrap();

        let mut second = sample_state();
        second.entries = vec![(
            1,
            Entry::present(
                Timestamp::new(
                    Hlc::new(PhysicalTime::from_millis(2), LogicalCounter::new(0)),
                    NodeId::new(0),
                ),
                "second".to_string(),
            ),
        )];
        backend.save(&second).unwrap();

        let loaded = backend.load().unwrap().unwrap();
        assert_eq!(loaded.entries[0].1.value(), Some(&"second".to_string()));
    }
    #[test]
    fn signed_peer_ids_round_trip_in_memory_and_through_bincode() {
        use crate::PeerId;

        let a = PeerId::from_bytes([0x11; 32]);
        let b = PeerId::from_bytes([0x22; 32]);
        let members = HashSet::from([a, b]);
        let acks = HashMap::from([(7_u32, HashMap::from([(a, 42_u64)]))]);
        let state =
            PersistedState::<u32, String, PeerId>::new(vec![], members.clone(), acks.clone());
        let backend = InMemoryPersistence::<u32, String, PeerId>::new();
        backend.save(&state).unwrap();
        let loaded = backend.load().unwrap().unwrap();
        assert_eq!(loaded.members, members);
        assert_eq!(loaded.tombstone_acks, acks);

        let encoded = bincode::serialize(&state).unwrap();
        let decoded: PersistedState<u32, String, PeerId> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.members, members);
        assert_eq!(decoded.tombstone_acks, acks);
        // The serialized state stores stable logical IDs; substrate routing is independent.
        let old_route = (1_u16, 3_u8);
        let new_route = (1_u16, 7_u8);
        assert_ne!(old_route, new_route);
        assert!(decoded.members.contains(&a));
        assert_eq!(decoded.tombstone_acks[&7_u32][&a], 42);
    }

    #[test]
    fn delta_types_support_signed_peer_ids_without_ip_address_translation() {
        use crate::PeerId;

        let a = PeerId::from_bytes([0x77; 32]);
        let delta: PersistenceDelta<u32, String, PeerId> = PersistenceDelta::new(
            HashMap::new(),
            HashMap::from([(a, true)]),
            HashSet::new(),
            HashMap::from([(9_u32, HashMap::from([(a, Some(123_u64))]))]),
        );
        assert_eq!(delta.members.get(&a), Some(&true));
        assert_eq!(
            delta.ack_peers.get(&9).and_then(|peers| peers.get(&a)),
            Some(&Some(123))
        );
    }
}
