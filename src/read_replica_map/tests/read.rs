// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use super::super::*;
use super::ephemeral_config;
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::FingerprintTreeMap;
use rsos::Fingerprint;

/// `get` returns the live value, and absent keys are `None`.
#[tokio::test]
async fn get_returns_integrated_value() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    assert!(read_replica.get(&1).is_none());
    read_replica.integrate(vec![(1, State::Present("hello".to_string()))]);
    assert_eq!(read_replica.get(&1).as_deref(), Some(&"hello".to_string()));
    assert!(read_replica.contains_key(&1));
    assert_eq!(read_replica.len(), 1);
}

/// `snapshot` (#34) hands back an `Arc` over the exact same tree `get`/`len` read from: a live
/// entry integrated before the snapshot was taken is visible in it, and a tombstone is present
/// (not filtered) since `snapshot` exposes the raw `State` wire representation.
#[tokio::test]
async fn snapshot_reflects_integrated_state_tombstones_included() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    read_replica.integrate(vec![
        (1, State::Present("hello".to_string())),
        (2, State::Tombstone),
    ]);

    let snapshot = read_replica.snapshot();
    assert_eq!(
        snapshot.get(&1).and_then(State::as_value),
        Some(&"hello".to_string())
    );
    assert!(snapshot.get(&2).is_some_and(State::is_tombstone));
    assert!(snapshot.get(&3).is_none());

    // A later integration must not retroactively change this already-taken snapshot.
    read_replica.integrate(vec![(1, State::Present("world".to_string()))]);
    assert_eq!(
        snapshot.get(&1).and_then(State::as_value),
        Some(&"hello".to_string()),
        "snapshot must still reflect the tree as it stood when it was taken"
    );
    assert_eq!(read_replica.get_cloned(&1), Some("world".to_string()));
}

/// `get_cloned` mirrors `get`, but owns rather than borrows: present for an integrated key,
/// `None` for one that was never integrated.
#[tokio::test]
async fn get_cloned_returns_an_owned_copy_or_none_for_a_missing_key() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    assert_eq!(read_replica.get_cloned(&1), None);
    read_replica.integrate(vec![(1, State::Present("hello".to_string()))]);
    assert_eq!(read_replica.get_cloned(&1), Some("hello".to_string()));
    assert_eq!(read_replica.get_cloned(&2), None);
}

/// A replicated tombstone (`State::Tombstone`) hides the value but is still a stored entry.
#[tokio::test]
async fn replicates_tombstones() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    read_replica.integrate(vec![(1, State::Present("v".to_string()))]);
    assert_eq!(read_replica.get(&1).as_deref(), Some(&"v".to_string()));

    // A later tombstone overwrites it: the value disappears from `get`, and it no longer counts
    // as a live entry (a read replica has no timestamp and trusts the authoritative peer).
    read_replica.integrate(vec![(1, State::Tombstone)]);
    assert!(read_replica.get(&1).is_none());
    assert!(!read_replica.contains_key(&1));
    assert_eq!(read_replica.len(), 0, "the tombstone is not a live entry");
    // The tombstone itself is still retained internally (the tree keeps it until the dated peer
    // observes it acknowledged and moves on) — `len` deliberately doesn't surface that raw size.
    assert_eq!(
        read_replica.tree.load_full().len(),
        1,
        "the tombstone is retained as a tree entry"
    );
}

/// The collection-shaped read API (`for_each`/`for_each_in_range`/`to_vec`/`range_to_vec`/
/// `keys`/`values`) mirrors [`ReplicatedMap`](crate::ReplicatedMap)'s: live entries only, in key
/// order, tombstones excluded.
#[tokio::test]
async fn collection_reads_exclude_tombstones() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    read_replica.integrate(vec![
        (1, State::Present(10)),
        (2, State::Present(20)),
        (3, State::Tombstone),
        (4, State::Present(40)),
    ]);

    assert_eq!(read_replica.to_vec(), vec![(1, 10), (2, 20), (4, 40)]);
    assert_eq!(read_replica.keys(), vec![1, 2, 4]);
    assert_eq!(read_replica.values(), vec![10, 20, 40]);
    assert_eq!(read_replica.range_to_vec(2..=3), vec![(2, 20)]);
    assert!(read_replica.range_to_vec(3..3).is_empty());

    let mut collected = Vec::new();
    read_replica.for_each(|k, v| collected.push((*k, *v)));
    assert_eq!(collected, read_replica.to_vec());

    let mut in_range = Vec::new();
    read_replica.for_each_in_range(2.., |k, v| in_range.push((*k, *v)));
    assert_eq!(in_range, vec![(2, 20), (4, 40)]);
}

/// `first_key_value`/`last_key_value` skip a tombstone sitting at the extremal raw key,
/// mirroring [`ReplicatedMap`](crate::ReplicatedMap)'s.
#[tokio::test]
async fn first_and_last_key_value_skip_boundary_tombstones() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    assert_eq!(read_replica.first_key_value(), None);
    assert_eq!(read_replica.last_key_value(), None);

    read_replica.integrate(vec![
        (1, State::Tombstone),
        (2, State::Present(20)),
        (3, State::Present(30)),
        (4, State::Present(40)),
        (5, State::Tombstone),
    ]);
    assert_eq!(read_replica.first_key_value(), Some((2, 20)));
    assert_eq!(read_replica.last_key_value(), Some((4, 40)));

    read_replica.integrate(vec![(2, State::Tombstone), (4, State::Tombstone)]);
    assert_eq!(read_replica.first_key_value(), Some((3, 30)));
    assert_eq!(read_replica.last_key_value(), Some((3, 30)));

    read_replica.integrate(vec![(3, State::Tombstone)]);
    assert_eq!(read_replica.first_key_value(), None);
    assert_eq!(read_replica.last_key_value(), None);
}

/// The read replica's value-only fingerprint matches an independently-built tree of the same
/// logical content — i.e. timestamps genuinely play no part in the hash.
#[tokio::test]
async fn value_fingerprint_is_timestamp_independent() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    read_replica.integrate(vec![
        (1, State::Present("a".to_string())),
        (2, State::Tombstone),
    ]);

    let mut reference: FingerprintTreeMap<i32, State<String>> = FingerprintTreeMap::new();
    reference.insert(1, State::Present("a".to_string()));
    reference.insert(2, State::Tombstone);

    assert_eq!(
        read_replica.value_fingerprint(..),
        reference.aggregate(..).fingerprint()
    );
}

/// #294: the deprecated `fingerprint` alias must actually forward to `value_fingerprint`, not
/// just compile — a mutant that no-ops it and returns a default `Fingerprint` would pass any
/// test that never compares its result to the real one.
#[test]
#[allow(deprecated)]
fn deprecated_fingerprint_alias_matches_value_fingerprint() {
    let read_replica = ReadReplicaMap::<i32, String>::new_with_transport(
        ephemeral_config(),
        Arc::new(crate::transport::InMemoryNetwork::new().bind("127.0.5.1:1".parse().unwrap())),
    );
    read_replica.integrate(vec![(1, State::Present("a".to_string()))]);
    assert_eq!(
        read_replica.fingerprint(..),
        read_replica.value_fingerprint(..)
    );
    assert_ne!(read_replica.fingerprint(..), Fingerprint::default());
}

/// A live value and its `State` projection hash identically only via the value-only basis:
/// per-entry, the dateless read replica saves the whole `Timestamp` (the point of the dateless
/// read replica).
#[test]
fn value_only_is_smaller_per_entry() {
    let dated = std::mem::size_of::<Entry<Timestamp, u64>>();
    let light = std::mem::size_of::<State<u64>>();
    assert!(
        light < dated,
        "value-only entry ({light} B) should be smaller than dated entry ({dated} B)"
    );
}

/// `is_empty` is `false` once a live (non-tombstone) value is integrated.
#[tokio::test]
async fn is_empty_is_false_once_a_live_value_is_integrated() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    assert!(read_replica.is_empty());

    read_replica.integrate(vec![(1, State::Present("hello".to_string()))]);
    assert!(!read_replica.is_empty());
}
