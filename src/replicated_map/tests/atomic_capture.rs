// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A full checkpoint must not splice roots or causal metadata across a writer's atomic batch.

use std::collections::HashMap;
use std::sync::{Arc, Barrier};

use super::{virtual_config, virtual_map};

#[test]
fn full_capture_pins_one_root_and_its_membership_and_acknowledgments() {
    let store = virtual_map::<u32, u32>(virtual_config());
    let end = super::super::persistence::SNAPSHOT_CHUNK_SIZE as u32;
    store.just_insert_bulk(&(0..=end).map(|key| (key, 0)).collect::<Vec<_>>());
    let peer = "127.0.0.9".parse().unwrap();
    store.engine.members.write().insert(peer);
    store
        .engine
        .tombstone_acks
        .write()
        .insert(end, HashMap::from([(peer, 11)]));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let capture = store.clone();
    let (worker_entered, worker_release) = (entered_tx, release.clone());
    let handle = std::thread::spawn(move || {
        let mut first = true;
        capture.collect_persisted_state_observed(|| {
            if first {
                first = false;
                worker_entered.send(()).unwrap();
                worker_release.wait();
            }
        })
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("worker did not reach the controlled boundary");
    store.just_insert_bulk(&[(0, 1), (end, 1)]);
    store.engine.decommission_peer(peer);
    release.wait();
    let saved = handle.join().unwrap();
    assert_eq!(
        saved
            .entries
            .iter()
            .find(|(key, _)| *key == 0)
            .unwrap()
            .1
            .value(),
        Some(&0)
    );
    assert_eq!(
        saved
            .entries
            .iter()
            .find(|(key, _)| *key == end)
            .unwrap()
            .1
            .value(),
        Some(&0)
    );
    assert!(
        saved.members.contains(&peer),
        "membership belongs to the pinned cut"
    );
    assert_eq!(saved.tombstone_acks[&end][&peer], 11);
    assert_eq!(store.get_cloned(&0), Some(1));
    assert_eq!(store.get_cloned(&end), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutating_callback_cannot_publish_the_projection_before_its_generation() {
    let store = virtual_map::<u32, u32>(virtual_config());
    store.just_insert(1, 10);
    let generation = store.engine.snapshot_generations.mutation();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let (worker_entered, worker_release) = (entered_tx, release.clone());
    store
        .engine
        .snapshot_generations
        .before_next_mutation(move || {
            worker_entered.send(()).unwrap();
            worker_release.wait();
        });
    let writer = store.clone();
    let runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _runtime = runtime.enter();
        writer.get_mut(&1, |value| *value.unwrap() = 20);
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("worker did not reach the controlled boundary");
    let dated = store.engine.map.load_full().get(&1).unwrap().project();
    let projected = store.engine.projection.load_full().get(&1).unwrap().clone();
    release.wait();
    drop(generation);
    handle.join().unwrap();
    assert_eq!(
        dated, projected,
        "a captured generation must contain matching dated and projected roots"
    );
    assert_eq!(store.get_cloned(&1), Some(20));
}

#[test]
fn receipt_only_gc_has_no_background_sweep_and_retains_the_candidate() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let store = virtual_map::<u32, u32>(virtual_config());
    // A test-only local insertion avoids needing a Tokio runtime just to broadcast a delete.
    store.just_remove(&7);
    let before = store.fingerprint(..);
    let future = store.clear_expired_tombstones();
    let mut future = std::pin::pin!(future);
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(())
    ));
    assert_eq!(store.fingerprint(..), before);
    assert!(store.snapshot().get(&7).unwrap().is_tombstone());
}
