// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::clock::Timestamp;
use crate::entry::Entry;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EntryDelta<V> {
    Upsert(Entry<Timestamp, V>),
    PhysicalDelete,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotGeneration<K, V> {
    pub(crate) id: u64,
    pub(crate) entries: HashMap<K, EntryDelta<V>>,
    pub(crate) members: HashMap<IpAddr, bool>,
    pub(crate) ack_key_clears: HashSet<K>,
    pub(crate) ack_peers: HashMap<K, HashMap<IpAddr, Option<u64>>>,
    pub(crate) raw_changes: usize,
}

impl<K, V> SnapshotGeneration<K, V> {
    fn is_empty(&self) -> bool {
        self.raw_changes == 0
            && self.entries.is_empty()
            && self.members.is_empty()
            && self.ack_key_clears.is_empty()
            && self.ack_peers.is_empty()
    }
}

impl<K, V> Default for SnapshotGeneration<K, V> {
    fn default() -> Self {
        Self {
            id: 0,
            entries: HashMap::new(),
            members: HashMap::new(),
            ack_key_clears: HashSet::new(),
            ack_peers: HashMap::new(),
            raw_changes: 0,
        }
    }
}

struct GenerationState<K, V> {
    next_id: u64,
    open: SnapshotGeneration<K, V>,
    frozen: Option<Arc<SnapshotGeneration<K, V>>>,
    tracking_enabled: bool,
}

impl<K, V> Default for GenerationState<K, V> {
    fn default() -> Self {
        Self {
            next_id: 1,
            open: SnapshotGeneration::default(),
            frozen: None,
            tracking_enabled: true,
        }
    }
}

/// Bounded two-generation journal: at most one immutable retryable generation plus the current
/// open generation. All durable mutation sinks take mutation() before changing runtime state;
/// freeze() takes the same mutex, making the cut atomic.
pub(crate) struct GenerationTracker<K, V> {
    state: Mutex<GenerationState<K, V>>,
}

impl<K, V> Default for GenerationTracker<K, V> {
    fn default() -> Self {
        Self {
            state: Mutex::new(GenerationState::default()),
        }
    }
}

impl<K: Eq + Hash, V> GenerationTracker<K, V> {
    pub(crate) fn mutation(&self) -> GenerationMutation<'_, K, V> {
        GenerationMutation {
            state: self.state.lock(),
        }
    }

    pub(crate) fn freeze(&self) -> Option<Arc<SnapshotGeneration<K, V>>> {
        let mut state = self.state.lock();
        if let Some(frozen) = &state.frozen {
            return Some(frozen.clone());
        }
        if state.open.is_empty() {
            return None;
        }

        let mut generation = std::mem::take(&mut state.open);
        generation.id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        let generation = Arc::new(generation);
        state.frozen = Some(generation.clone());
        Some(generation)
    }

    pub(crate) fn commit(&self, generation_id: u64) -> bool {
        let mut state = self.state.lock();
        if state.frozen.as_ref().map(|generation| generation.id) != Some(generation_id) {
            return false;
        }
        state.frozen = None;
        true
    }

    pub(crate) fn set_tracking_enabled(&self, enabled: bool) {
        self.state.lock().tracking_enabled = enabled;
    }

    pub(crate) fn reset_clean(&self) {
        let mut state = self.state.lock();
        state.open = SnapshotGeneration::default();
        state.frozen = None;
        state.tracking_enabled = true;
    }

    #[cfg(test)]
    fn retained_generations(&self) -> usize {
        let state = self.state.lock();
        usize::from(state.frozen.is_some()) + usize::from(!state.open.is_empty())
    }
}

pub(crate) struct GenerationMutation<'a, K, V> {
    state: MutexGuard<'a, GenerationState<K, V>>,
}

impl<K: Eq + Hash, V> GenerationMutation<'_, K, V> {
    fn enabled(&self) -> bool {
        self.state.tracking_enabled
    }

    pub(crate) fn record_entry(&mut self, key: K, entry: Entry<Timestamp, V>) {
        if !self.enabled() {
            return;
        }
        self.state.open.entries.insert(key, EntryDelta::Upsert(entry));
        self.state.open.raw_changes = self.state.open.raw_changes.saturating_add(1);
    }

    pub(crate) fn record_physical_delete(&mut self, key: K) {
        if !self.enabled() {
            return;
        }
        self.state.open.entries.insert(key, EntryDelta::PhysicalDelete);
        self.state.open.raw_changes = self.state.open.raw_changes.saturating_add(1);
    }

    pub(crate) fn record_member(&mut self, peer: IpAddr, present: bool) {
        if !self.enabled() {
            return;
        }
        self.state.open.members.insert(peer, present);
        self.state.open.raw_changes = self.state.open.raw_changes.saturating_add(1);
    }

    pub(crate) fn record_ack(&mut self, key: K, peer: IpAddr, version: Option<u64>) {
        if !self.enabled() {
            return;
        }
        self.state
            .open
            .ack_peers
            .entry(key)
            .or_default()
            .insert(peer, version);
        self.state.open.raw_changes = self.state.open.raw_changes.saturating_add(1);
    }

    pub(crate) fn record_ack_key_clear(&mut self, key: K) {
        if !self.enabled() {
            return;
        }
        self.state.open.ack_key_clears.insert(key.clone());
        self.state.open.ack_peers.remove(&key);
        self.state.open.raw_changes = self.state.open.raw_changes.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime};
    use crate::entry::State;

    fn entry(value: u64, millis: u64) -> Entry<Timestamp, u64> {
        Entry {
            stamp: Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(millis), LogicalCounter::ZERO),
                NodeId::new(7),
            ),
            state: State::Value(value),
        }
    }

    #[test]
    fn same_key_mutations_coalesce_to_final_effect() {
        let tracker = GenerationTracker::<u64, u64>::default();
        {
            let mut mutation = tracker.mutation();
            mutation.record_entry(1, entry(10, 1));
            mutation.record_entry(1, entry(20, 2));
            mutation.record_physical_delete(1);
            mutation.record_entry(1, entry(30, 3));
        }

        let frozen = tracker.freeze().expect("dirty generation");
        assert_eq!(frozen.raw_changes, 4);
        assert_eq!(frozen.entries.len(), 1);
        assert_eq!(
            frozen.entries.get(&1),
            Some(&EntryDelta::Upsert(entry(30, 3)))
        );
    }

    #[test]
    fn failed_generation_retries_while_new_writes_stay_open() {
        let tracker = GenerationTracker::<u64, u64>::default();
        tracker.mutation().record_entry(1, entry(10, 1));

        let first = tracker.freeze().expect("first generation");
        assert_eq!(first.id, 1);

        tracker.mutation().record_entry(2, entry(20, 2));
        let retry = tracker.freeze().expect("retry generation");
        assert!(Arc::ptr_eq(&first, &retry));
        assert_eq!(tracker.retained_generations(), 2);

        assert!(tracker.commit(first.id));
        let second = tracker.freeze().expect("newer open generation");
        assert_eq!(second.id, 2);
        assert!(second.entries.contains_key(&2));
        assert!(!second.entries.contains_key(&1));
    }

    #[test]
    fn membership_and_ack_mutations_coalesce_without_losing_clear_order() {
        let tracker = GenerationTracker::<u64, u64>::default();
        let peer: IpAddr = "127.0.0.2".parse().unwrap();
        {
            let mut mutation = tracker.mutation();
            mutation.record_member(peer, true);
            mutation.record_member(peer, false);
            mutation.record_ack(7, peer, Some(10));
            mutation.record_ack_key_clear(7);
            mutation.record_ack(7, peer, Some(11));
        }

        let frozen = tracker.freeze().expect("dirty generation");
        assert_eq!(frozen.members.get(&peer), Some(&false));
        assert!(frozen.ack_key_clears.contains(&7));
        assert_eq!(
            frozen.ack_peers.get(&7).and_then(|acks| acks.get(&peer)),
            Some(&Some(11))
        );
    }

    #[test]
    fn restored_state_can_be_installed_without_becoming_dirty() {
        let tracker = GenerationTracker::<u64, u64>::default();
        tracker.set_tracking_enabled(false);
        tracker.mutation().record_entry(1, entry(10, 1));
        tracker.reset_clean();
        assert!(tracker.freeze().is_none());

        tracker.mutation().record_entry(2, entry(20, 2));
        assert!(tracker.freeze().is_some());
    }
}
