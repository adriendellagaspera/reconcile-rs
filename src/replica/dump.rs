// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Instant;

use rbsr::EnumerationRange;
use tracing::trace;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};

use super::pacing::DumpChannel;
use super::{Message, Replica};

impl<K: Key + Hash, V: Value> Replica<K, V> {
    pub(super) fn start_dated_dump(
        &self,
        differences: Vec<EnumerationRange<K>>,
        peer: SocketAddr,
        comparison_fingerprint: [u8; 32],
    ) {
        let Some((peer_guard, global_guard)) = self.try_claim_dump_slot(peer) else {
            self.stash_pending_dump(DumpChannel::Dated, peer, differences);
            return;
        };

        if let Err(reason) = self.bulk_admission.try_admit_at(
            peer.ip(),
            DumpChannel::Dated,
            comparison_fingerprint,
            Instant::now(),
        ) {
            trace!("skipped bulk dump to {peer}: admission denied ({reason:?})");
            return;
        }

        let updates: Vec<Message<K, Entry<Timestamp, V>, State<V>>> = {
            let guard = self.map.load_full();
            let mut updates = Vec::new();
            for range in differences {
                for (key, value) in guard.range(range) {
                    updates.push(Message::EntryUpdate((key.clone(), value.clone())));
                }
            }
            updates
        };

        if updates.is_empty() {
            return;
        }

        self.spawn_paced_send(updates, peer, peer_guard, global_guard, DumpChannel::Dated);
    }

    pub(super) fn start_value_dump(
        &self,
        differences: Vec<EnumerationRange<K>>,
        peer: SocketAddr,
        comparison_fingerprint: [u8; 32],
    ) {
        let Some((peer_guard, global_guard)) = self.try_claim_dump_slot(peer) else {
            self.stash_pending_dump(DumpChannel::ValueOnly, peer, differences);
            return;
        };

        if let Err(reason) = self.bulk_admission.try_admit_at(
            peer.ip(),
            DumpChannel::ValueOnly,
            comparison_fingerprint,
            Instant::now(),
        ) {
            trace!("skipped value-only bulk dump to {peer}: admission denied ({reason:?})");
            return;
        }

        let updates: Vec<Message<K, Entry<Timestamp, V>, State<V>>> = {
            let guard = self.projection.load_full();
            let mut updates = Vec::new();
            for range in differences {
                for (key, value) in guard.range(range) {
                    updates.push(Message::StateUpdate((key.clone(), value.clone())));
                }
            }
            updates
        };

        if updates.is_empty() {
            return;
        }

        self.spawn_paced_send(
            updates,
            peer,
            peer_guard,
            global_guard,
            DumpChannel::ValueOnly,
        );
    }
}
