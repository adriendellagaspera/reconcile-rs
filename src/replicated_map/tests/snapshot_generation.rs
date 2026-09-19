// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #46: a save that succeeds must not retire a write that committed *during* the save.
//! Two snapshots initiated from cloned handles must serialize their backend calls.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use crate::persistence::{PersistedState, Persistence};
use crate::ReplicatedMap;

use super::ephemeral_config;

struct PausedSave {
    block_next: AtomicBool,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    saved: Mutex<Vec<PersistedState<u32, u32>>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl PausedSave {
    fn new(block_first: bool) -> Self {
        Self {
            block_next: AtomicBool::new(block_first),
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
            saved: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        }
    }
}

impl Persistence<u32, u32> for PausedSave {
    fn load(&self) -> io::Result<Option<PersistedState<u32, u32>>> {
        Ok(self.saved.lock().unwrap().last().cloned())
    }

    fn save(&self, state: &PersistedState<u32, u32>) -> io::Result<()> {
        let active = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(active, Ordering::AcqRel);
        if self.block_next.swap(false, Ordering::AcqRel) {
            self.entered.wait();
            self.release.wait();
        }
        self.saved.lock().unwrap().push(state.clone());
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test]
async fn write_during_save_stays_pending_and_is_recovered_by_next_snapshot() {
    let backend = Arc::new(PausedSave::new(true));
    let store = ReplicatedMap::<u32, u32>::new(ephemeral_config().with_snapshot_interval(None))
        .await
        .unwrap()
        .with_persistence(backend.clone())
        .unwrap();

    store.just_insert(1, 10);
    let first = store.clone();
    let handle = std::thread::spawn(move || first.snapshot_now().unwrap());
    backend.entered.wait(); // the first save holds an old state (only key 1)
    store.just_insert(2, 20); // commits before save returns
    backend.release.wait();
    handle.join().unwrap();

    assert_eq!(store.engine.change_count(), 1, "post-capture write lost");
    let first_saved = backend.saved.lock().unwrap()[0].clone();
    assert_eq!(first_saved.entries.len(), 1);

    store.snapshot_now().unwrap();
    assert_eq!(store.engine.change_count(), 0);
    let persisted = backend.saved.lock().unwrap().last().unwrap().clone();
    assert_eq!(persisted.entries.len(), 2);
    assert!(persisted.entries.iter().any(|(key, _)| *key == 2));

    let restarted = ReplicatedMap::<u32, u32>::new(ephemeral_config())
        .await
        .unwrap()
        .with_persistence(backend)
        .unwrap();
    assert_eq!(restarted.get_cloned(&1), Some(10));
    assert_eq!(restarted.get_cloned(&2), Some(20));
    assert_eq!(restarted.fingerprint(..), store.fingerprint(..));
}

#[tokio::test]
async fn cloned_handles_do_not_overlap_backend_saves() {
    let backend = Arc::new(PausedSave::new(true));
    let store = ReplicatedMap::<u32, u32>::new(ephemeral_config().with_snapshot_interval(None))
        .await
        .unwrap()
        .with_persistence(backend.clone())
        .unwrap();

    store.just_insert(1, 10);
    let a = store.clone();
    let first = std::thread::spawn(move || a.snapshot_now().unwrap());
    backend.entered.wait();
    let b = store.clone();
    let second = std::thread::spawn(move || b.snapshot_now().unwrap());
    // The first remains paused until released. The second must serialize with it:
    // max_in_flight catches overlaps without relying on scheduling or sleeps.
    backend.release.wait();
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(backend.saved.lock().unwrap().len(), 2);
    assert_eq!(backend.max_in_flight.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn metadata_only_decommission_and_ack_forget_remain_snapshot_pending() {
    let backend = Arc::new(PausedSave::new(false));
    let store = ReplicatedMap::<u32, u32>::new(ephemeral_config().with_snapshot_interval(None))
        .await
        .unwrap()
        .with_persistence(backend.clone())
        .unwrap();

    store.just_insert(1, 10);
    store.just_remove(&1);
    store.just_insert(2, 20);
    let peer = "127.0.0.9".parse().unwrap();
    store.engine.members.write().insert(peer);
    store
        .engine
        .tombstone_acks
        .write()
        .insert(1, std::collections::HashMap::from([(peer, 7)]));
    store.snapshot_now().unwrap();
    assert_eq!(store.engine.change_count(), 0);

    store.engine.decommission_peer(peer);
    assert_eq!(
        store.engine.change_count(),
        2,
        "membership and ack removals were not tracked"
    );
    store.snapshot_now().unwrap();
    let restored = ReplicatedMap::<u32, u32>::new(ephemeral_config())
        .await
        .unwrap()
        .with_persistence(backend.clone())
        .unwrap();
    assert!(!restored.engine.members.read().contains(&peer));
    assert!(!restored
        .engine
        .tombstone_acks
        .read()
        .get(&1)
        .unwrap()
        .contains_key(&peer));
    assert!(restored
        .engine
        .map
        .load_full()
        .get(&1)
        .unwrap()
        .is_tombstone());

    // Removing ack bookkeeping alone must be visible to the next generation,
    // without a corresponding dated-map modification.
    store
        .engine
        .tombstone_acks
        .write()
        .insert(1, std::collections::HashMap::from([(peer, 7)]));
    store.snapshot_now().unwrap();
    store.engine.forget_tombstone(&1);
    assert_eq!(store.engine.change_count(), 1);
    store.snapshot_now().unwrap();
    let last = backend.saved.lock().unwrap().last().unwrap().clone();
    assert!(!last.tombstone_acks.contains_key(&1));
}
