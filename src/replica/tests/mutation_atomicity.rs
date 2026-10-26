// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Controlled schedules at the generation boundary; no timing-based lock-order inference.

use std::sync::{Arc, Barrier};

use crate::entry::Entry;
use crate::replicated_map::Config;

use super::in_memory_test_replica;

#[test]
fn a_writer_waiting_for_the_generation_boundary_does_not_hold_the_write_lock() {
    let replica = in_memory_test_replica::<u32, u32>(Config::default().with_insecure_no_key());
    let generation = replica.snapshot_generations.mutation();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let (worker_entered, worker_release) = (entered_tx, release.clone());
    replica.snapshot_generations.before_next_mutation(move || {
        worker_entered.send(()).unwrap();
        worker_release.wait();
    });
    let writer = replica.clone();
    let handle = std::thread::spawn(move || {
        writer.just_insert(7, Entry::present(writer.clock_now(), 42));
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("worker did not reach the controlled boundary");
    let write_lock_is_free = replica.write_lock.try_lock().is_some();
    release.wait();
    drop(generation);
    handle.join().unwrap();
    assert!(
        write_lock_is_free,
        "generation boundary must precede the map write lock"
    );
}

#[test]
fn a_reinserted_value_survives_collection_of_an_earlier_tombstone_candidate() {
    let replica = in_memory_test_replica::<u32, u32>(Config::default().with_insecure_no_key());
    replica.just_insert(7, Entry::tombstone(replica.clock_now()));
    let candidate = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let collector = replica.clone();
    let (worker_candidate, worker_release) = (candidate.clone(), release.clone());
    let handle = std::thread::spawn(move || {
        let version = super::super::version_hash(collector.map.load_full().get(&7).unwrap());
        assert!(collector.is_tombstone_stable(&7, version));
        let expected = collector.map.load_full().get(&7).unwrap().stamp;
        worker_candidate.wait();
        worker_release.wait();
        collector.collect_candidate(
            &7,
            expected,
            chrono::Utc::now() + chrono::Duration::seconds(1),
            || std::time::Duration::ZERO,
        )
    });
    candidate.wait();
    replica.just_insert(7, Entry::present(replica.clock_now(), 99));
    release.wait();
    handle.join().unwrap();
    assert_eq!(
        replica.map.load_full().get(&7).and_then(Entry::value),
        Some(&99)
    );
}
