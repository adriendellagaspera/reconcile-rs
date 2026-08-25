// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::entry::{Entry, State};
use crate::ReplicatedMap;

use super::ephemeral_config;

/// `snapshot` (#34) hands back an `Arc` over the exact same dated tree `get` reads from,
/// tombstones included (it exposes the raw `Entry` wire representation, unlike `to_vec`). A
/// later write on the same handle must not retroactively change an already-taken snapshot --
/// that is the whole point of it being an owned `Arc`, not a lock.
#[tokio::test]
async fn snapshot_reflects_live_state_tombstones_included_and_is_immutable_once_taken() {
    let store = ReplicatedMap::<i32, i32>::new(ephemeral_config())
        .await
        .unwrap();
    store.insert(1, 10);
    store.insert(2, 20);
    store.remove(&2);

    let snapshot = store.snapshot();
    assert_eq!(snapshot.get(&1).and_then(Entry::value), Some(&10));
    assert!(snapshot.get(&2).is_some_and(Entry::is_tombstone));
    assert!(snapshot.get(&3).is_none());

    store.insert(1, 99);
    assert_eq!(
        snapshot.get(&1).and_then(Entry::value),
        Some(&10),
        "snapshot must still reflect the tree as it stood when it was taken"
    );
    assert_eq!(store.get_cloned(&1), Some(99));
}

/// `value_snapshot` mirrors `snapshot` but over the timestamp-less projection -- the same tree
/// `value_fingerprint` measures -- so a live entry's projected `State::Present` is visible in
/// it, independent of the dated `snapshot`.
#[tokio::test]
async fn value_snapshot_reflects_the_projection_not_the_dated_map() {
    let store = ReplicatedMap::<i32, i32>::new(ephemeral_config())
        .await
        .unwrap();
    store.insert(1, 10);

    let snapshot = store.value_snapshot();
    assert_eq!(
        snapshot.get(&1).and_then(State::as_value),
        Some(&10),
        "value_snapshot must expose the value-only projection"
    );
}

/// `get_cloned` must not hold the read lock past its return, so a write immediately
/// following it (the `get`-then-`insert` pattern `get`'s own guard would self-deadlock on)
/// completes without hanging.
#[tokio::test]
async fn get_cloned_does_not_hold_the_lock_across_a_following_write() {
    let store = ReplicatedMap::<i32, i32>::new(ephemeral_config())
        .await
        .unwrap();
    store.insert(1, 10);

    let value = store.get_cloned(&1);
    assert_eq!(value, Some(10));
    // If `get_cloned` still held the read lock here, this write lock acquisition would hang
    // forever instead of returning.
    store.insert(1, 20);

    assert_eq!(store.get_cloned(&1), Some(20));
    assert_eq!(store.get_cloned(&2), None);
}
