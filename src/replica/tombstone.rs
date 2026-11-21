// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Tombstone receipt bookkeeping for the legacy runtime.
//!
//! Receipt ACKs do not prove that rolled-back or delayed application history can no longer be
//! admitted. Physical tombstone collection is therefore not implemented.

use std::hash::Hash;
use std::net::IpAddr;

use serde::Serialize;

use crate::bounds::{Key, Value};

use super::Replica;

/// A deterministic, cross-node version token for a value: the low 64 bits of `rsos::digest`.
/// A peer acknowledges the exact tombstone version it holds, so a stale ACK cannot be mistaken
/// for receipt of a newer tombstone.
pub(crate) fn version_hash<V: Serialize>(value: &V) -> u64 {
    rsos::digest(value).0[0]
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Whether `peer` still owes an ACK for some held tombstone.
    ///
    /// Walks the live tombstone index rather than the ACK map: a freshly deleted tombstone has no
    /// ACK entry yet and must still count as pending.
    pub(crate) fn has_pending_tombstone_acks(&self, peer: IpAddr) -> bool {
        let live = self.live_tombstones.read();
        if live.is_empty() {
            return false;
        }
        let map = self.map.load_full();
        let acks = self.tombstone_acks.read();
        live.iter().any(|key| {
            let Some(entry) = map.get(key) else {
                return false;
            };
            let version = version_hash(entry);
            acks.get(key).and_then(|peer_acks| peer_acks.get(&peer)) != Some(&version)
        })
    }
}
