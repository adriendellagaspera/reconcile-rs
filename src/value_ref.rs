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
use std::sync::Arc;

use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::FingerprintTreeMap;

/// Which backing tree a [`ValueRef`] was built over: [`ReplicatedMap`](crate::ReplicatedMap)'s
/// dated map, or [`ReadReplicaMap`](crate::ReadReplicaMap)'s value-only projection. `pub(crate)`
/// so `get()` in either module can construct one, but the shape stays opaque to callers (#297).
pub(crate) enum Snapshot<K, V> {
    Dated(Arc<FingerprintTreeMap<K, Entry<Timestamp, V>>>, K),
    Projected(Arc<FingerprintTreeMap<K, State<V>>>, K),
}

/// A snapshot-backed reference to a live value.
///
/// #34: owns an immutable `Arc` snapshot of the whole backing tree rather than holding a lock —
/// unlike the pre-#34, `RwLock`-guard-backed version, a `ValueRef` may be held indefinitely,
/// including across a write on the same handle, with no deadlock risk: the write installs a fresh
/// tree behind a new `Arc`, and this `ValueRef` still points at whichever tree was live when
/// `get` returned it. Derefs to `&V`.
pub struct ValueRef<K, V>(pub(crate) Snapshot<K, V>);

impl<K: Ord, V> Deref for ValueRef<K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        match &self.0 {
            Snapshot::Dated(snapshot, key) => snapshot
                .get(key)
                .and_then(|entry| entry.value())
                .expect("ValueRef always wraps a key live in the snapshot it was built from"),
            Snapshot::Projected(snapshot, key) => snapshot
                .get(key)
                .and_then(|state| state.as_value())
                .expect("ValueRef always wraps a key live in the snapshot it was built from"),
        }
    }
}
