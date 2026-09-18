// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`ValueRef`]: the handle [`ReplicatedMap::get`](crate::ReplicatedMap::get) and
//! [`ReadReplicaMap::get`](crate::ReadReplicaMap::get) return.

use std::ops::Deref;

use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use rsos::OwnedValueRef;

/// Which backing tree a [`ValueRef`] was built over: [`ReplicatedMap`](crate::ReplicatedMap)'s
/// dated map, or [`ReadReplicaMap`](crate::ReadReplicaMap)'s value-only projection. Each variant
/// owns the exact persistent B-tree node containing the value, so dereferencing never repeats the
/// key lookup that created the handle.
pub(crate) enum Snapshot<K, V> {
    Dated(OwnedValueRef<K, Entry<Timestamp, V>>),
    Projected(OwnedValueRef<K, State<V>>),
}

/// A snapshot-backed reference to a live value.
///
/// Owns a persistent-node handle rather than a lock. A `ValueRef` may therefore be held
/// indefinitely, including across a write on the same map: the write forks a shared node before
/// mutating it and this handle continues to observe the version in which it was created. The
/// initial lookup is `O(log n)`; dereferencing the resulting handle is `O(1)`.
pub struct ValueRef<K, V>(pub(crate) Snapshot<K, V>);

impl<K, V> Deref for ValueRef<K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        match &self.0 {
            Snapshot::Dated(entry) => entry
                .value()
                .expect("ValueRef always wraps a live dated entry"),
            Snapshot::Projected(state) => state
                .as_value()
                .expect("ValueRef always wraps a live projected state"),
        }
    }
}
