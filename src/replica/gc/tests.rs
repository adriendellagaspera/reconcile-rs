// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;

use crate::replicated_map::Config;

use super::*;

#[test]
fn gc_collect_counts_physical_delete_and_ack_cleanup() {
    let config = Config::default()
        .with_port(5000)
        .with_listen_addr("127.0.0.120".parse().unwrap())
        .with_insecure_no_key();
    let replica = crate::replica::tests::in_memory_test_replica::<u32, u32>(config);
    let key = 7;
    let peer: IpAddr = "127.0.0.9".parse().unwrap();
    replica.just_insert(key, Entry::tombstone(replica.clock_now()));
    replica
        .tombstone_acks
        .write()
        .insert(key, HashMap::from([(peer, 11)]));
    let before = replica.change_count();

    let expected = replica.map.load_full().get(&key).unwrap().stamp;
    assert!(replica
        .collect_candidate(
            &key,
            expected,
            Utc::now() + chrono::Duration::seconds(1),
            || Duration::ZERO
        )
        .is_some());
    assert_eq!(
        replica.change_count() - before,
        2,
        "physical deletion and ACK cleanup are two durable mutations"
    );
}

/// Pins the exact formula (`rsos::digest`'s low limb), not just "some function of the
/// value" — a mutant hardcoding a constant return has no other caller in this crate that
/// would catch it (every existing test only compares `version_hash` against itself).
#[test]
fn stable_peer_ids_gate_gc_without_transport_endpoint_keys() {
    use crate::PeerId;

    let a = PeerId::from_bytes([0x41; 32]);
    let b = PeerId::from_bytes([0x42; 32]);
    let members = HashSet::from([a, b]);
    let key = 7_u32;
    let mut acknowledgments = HashMap::new();
    assert!(!all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        42
    ));

    acknowledgments.insert(key, HashMap::from([(a, 42_u64)]));
    assert!(!all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        42
    ));
    acknowledgments.get_mut(&key).unwrap().insert(b, 41);
    assert!(!all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        42
    ));
    acknowledgments.get_mut(&key).unwrap().insert(b, 42);
    assert!(all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        42
    ));
    assert!(!all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        43
    ));

    // Route updates must not alter the causal membership or ACK bookkeeping.
    let mut routes = HashMap::from([(a, (3_u16, 2_u8))]);
    routes.insert(a, (3, 7));
    assert!(all_members_acknowledged(
        &members,
        &acknowledgments,
        &key,
        42
    ));
    assert_eq!(routes.len(), 1);
    assert!(all_members_acknowledged::<u32, PeerId>(
        &HashSet::new(),
        &HashMap::new(),
        &key,
        42
    ));
}

#[test]
fn version_hash_matches_the_digest_low_limb() {
    assert_eq!(version_hash(&42u32), rsos::digest(&42u32).0[0]);
}

#[test]
fn version_hash_differs_for_different_values() {
    assert_ne!(version_hash(&1u32), version_hash(&2u32));
}

#[test]
fn local_collection_checks_current_version_retention_and_all_current_members() {
    use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime};
    let replica = crate::replica::tests::in_memory_test_replica::<u32, u32>(
        Config::default().with_insecure_no_key(),
    );
    let stamp = Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::ZERO),
        NodeId::new(1),
    );
    let entry = Entry::tombstone(stamp);
    let version = version_hash(&entry);
    replica.just_insert(7, entry);
    let peer: IpAddr = "127.0.0.9".parse().unwrap();
    replica.members.write().insert(peer);
    let now = Utc::now();
    assert!(replica
        .collect_candidate(&7, stamp, now, || Duration::ZERO)
        .is_none());
    replica
        .tombstone_acks
        .write()
        .insert(7, HashMap::from([(peer, version.wrapping_add(1))]));
    assert!(replica
        .collect_candidate(&7, stamp, now, || Duration::ZERO)
        .is_none());
    replica
        .tombstone_acks
        .write()
        .insert(7, HashMap::from([(peer, version)]));
    let wheel =
        crate::timeout_wheel::TimeoutWheel::<u32>::new().with_timeout(Duration::from_secs(1));
    let edge = chrono::DateTime::from_timestamp_millis(2_000).unwrap();
    assert!(replica
        .collect_candidate(&7, stamp, edge, || wheel.timeout())
        .is_none());
    let expired = edge + chrono::Duration::milliseconds(1);
    let before = replica.change_count();
    // Receipt completion is observable, but does not pass the production history gate.
    assert!(replica
        .gc_collect(&7, stamp, expired, || Duration::ZERO)
        .is_none());
    assert_eq!(replica.change_count(), before);
    assert!(replica
        .collect_candidate(&7, stamp, expired, || wheel.timeout())
        .is_some());
    assert!(replica.map.load_full().get(&7).is_none());
    assert!(replica.projection.load_full().get(&7).is_none());
    assert!(!replica.live_tombstones.read().contains(&7));
    assert!(!replica.tombstone_acks.read().contains_key(&7));
    assert_eq!(replica.change_count() - before, 2);
}

#[test]
fn an_older_candidate_cannot_collect_a_newer_tombstone() {
    let replica = crate::replica::tests::in_memory_test_replica::<u32, u32>(
        Config::default().with_insecure_no_key(),
    );
    let older = replica.clock_now();
    replica.just_insert(7, Entry::tombstone(older));
    let newer = replica.clock_now();
    replica.just_insert(7, Entry::tombstone(newer));
    assert_ne!(older, newer);
    assert!(replica
        .collect_candidate(&7, older, Utc::now() + chrono::Duration::seconds(1), || {
            Duration::ZERO
        })
        .is_none());
    assert_eq!(replica.map.load_full().get(&7).unwrap().stamp, newer);
}

#[test]
fn member_added_after_candidate_selection_is_a_collection_blocker() {
    use std::sync::{Arc, Barrier};
    let replica = crate::replica::tests::in_memory_test_replica::<u32, u32>(
        Config::default().with_insecure_no_key(),
    );
    let stamp = replica.clock_now();
    replica.just_insert(7, Entry::tombstone(stamp));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let (worker_entered, worker_release) = (entered_tx, release.clone());
    replica.snapshot_generations.before_next_mutation(move || {
        worker_entered.send(()).unwrap();
        worker_release.wait();
    });
    let collector = replica.clone();
    let handle = std::thread::spawn(move || {
        collector.collect_candidate(&7, stamp, Utc::now() + chrono::Duration::seconds(1), || {
            Duration::ZERO
        })
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("worker did not reach the controlled boundary");
    {
        let _generation = replica.snapshot_generations.mutation();
        replica.members.write().insert("127.0.0.9".parse().unwrap());
    }
    release.wait();
    assert!(handle.join().unwrap().is_none());
    assert!(replica.map.load_full().get(&7).unwrap().is_tombstone());
}
