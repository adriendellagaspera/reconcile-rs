// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use rbsr::RangeAggregate;
use tracing::{debug, instrument, trace, warn};

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::observability;
use gossip::auth;

use super::collision;
use super::pacing::DumpChannel;
use super::{send_messages_to, version_hash, Message, Replica, MAX_MESSAGES_PER_DATAGRAM};

struct DecodedDatagram<K, V> {
    dated_ranges: Vec<RangeAggregate<K>>,
    dated_updates: Vec<(K, Entry<Timestamp, V>)>,
    tombstone_acks: Vec<(K, u64)>,
    saw_convergence_ack: bool,
    value_ranges: Vec<RangeAggregate<K>>,
}

impl<K, V> DecodedDatagram<K, V> {
    fn spoke_dated(&self) -> bool {
        !self.dated_ranges.is_empty()
            || !self.dated_updates.is_empty()
            || !self.tombstone_acks.is_empty()
            || self.saw_convergence_ack
    }
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Handle the messages in an already-authenticated, replay-checked [`Payload`] — taking
    /// [`auth::Payload<auth::Verified>`](auth::Payload) rather than bytes makes an unchecked
    /// datagram unrepresentable here.
    ///
    /// Returns whether the datagram carried at least one **dated** message, which is what
    /// qualifies the sender for membership: a value-only sender is a read replica and must not
    /// gate tombstone GC.
    #[instrument(name = "reconcile.handle", skip_all, fields(peer = %peer))]
    pub(super) async fn handle_messages(
        &self,
        payload: auth::Payload<'_, auth::Verified>,
        peer: SocketAddr,
        send_buf: &mut Vec<u8>,
    ) -> bool {
        let timer = observability::timer();
        let Some(batch) = self.decode_datagram(payload, peer) else {
            return false;
        };
        let spoke_dated = batch.spoke_dated();

        self.record_tombstone_acks(batch.tombstone_acks, peer.ip());
        self.handle_dated_comparison(batch.dated_ranges, peer, send_buf)
            .await;
        self.apply_dated_updates(batch.dated_updates, peer, send_buf)
            .await;
        self.handle_value_comparison(batch.value_ranges, peer, send_buf)
            .await;

        observability::record_handle_duration(timer);
        spoke_dated
    }

    fn decode_datagram(
        &self,
        payload: auth::Payload<'_, auth::Verified>,
        peer: SocketAddr,
    ) -> Option<DecodedDatagram<K, V>> {
        let payload = payload.as_bytes();
        trace!("received {} bytes from {peer}", payload.len());
        // Bound the decoded message count so a crafted datagram cannot expand without limit.
        let messages: Vec<Message<K, Entry<Timestamp, V>, State<V>>> =
            match gossip::bincode::decode_stream(payload, MAX_MESSAGES_PER_DATAGRAM) {
                Ok(messages) => messages,
                Err(kind) => {
                    warn!("failed to deserialize datagram from {peer}, dropping it: {kind:?}");
                    observability::record_datagram_dropped("malformed");
                    return None;
                }
            };

        let mut batch = DecodedDatagram {
            dated_ranges: Vec::new(),
            dated_updates: Vec::new(),
            tombstone_acks: Vec::new(),
            saw_convergence_ack: false,
            value_ranges: Vec::new(),
        };
        for message in messages {
            match message {
                Message::EntryFingerprint(segment) => batch.dated_ranges.push(segment),
                Message::EntryUpdate(update) => batch.dated_updates.push(update),
                Message::TombstoneAck(ack) => batch.tombstone_acks.push(ack),
                Message::StateFingerprint(segment) => batch.value_ranges.push(segment),
                // A dated store is authoritative and never integrates a state-only update.
                Message::StateUpdate(_) => {}
                // The receive loop already clears a pending repair on any datagram from this peer;
                // the ack still counts as proof that the sender speaks the dated channel.
                Message::ConvergenceAck => batch.saw_convergence_ack = true,
                // This version owns no semantics for tag 6. Ignore it explicitly rather than via
                // a wildcard, so adding another real variant still forces this dispatch to change.
                Message::Reserved6(_) => {}
            }
        }
        Some(batch)
    }

    fn record_tombstone_acks(&self, acks: Vec<(K, u64)>, peer_ip: IpAddr) {
        if acks.is_empty() {
            return;
        }
        let map_guard = self.map.load_full();
        let mut guard = self.tombstone_acks.write();
        for (key, version) in acks {
            // Only acks for locally-held tombstones are retained, bounding the bookkeeping map.
            if map_guard.get(&key).is_some_and(|v| v.is_tombstone()) {
                guard.entry(key).or_default().insert(peer_ip, version);
            } else {
                trace!(
                    "dropped ack from {peer_ip} for key with no local tombstone;                      ignoring to prevent unbounded bookkeeping"
                );
            }
        }
    }

    async fn handle_dated_comparison(
        &self,
        in_comparison: Vec<RangeAggregate<K>>,
        peer: SocketAddr,
        send_buf: &mut Vec<u8>,
    ) {
        if in_comparison.is_empty() {
            return;
        }

        debug!("received {} segments", in_comparison.len());
        let mut differences = Vec::new();
        let mut out_comparison = Vec::new();
        {
            let guard = self.map.load_full();
            let mut rng = self.rng.write();
            rbsr::protocol_round(
                &*guard,
                in_comparison,
                &mut out_comparison,
                &mut differences,
                &mut rng,
            );
        }
        let converged_with_nothing_to_send = out_comparison.is_empty() && differences.is_empty();

        // Refinement comparison items are small and latency-sensitive: send them inline.
        if !out_comparison.is_empty() {
            debug!("returning {} segments", out_comparison.len());
            trace!("segments: {out_comparison:?}");
            let messages: Vec<_> = out_comparison
                .into_iter()
                .map(Message::EntryFingerprint::<K, Entry<Timestamp, V>, State<V>>)
                .collect();
            send_messages_to(&messages, &self.send_ports(), &peer, send_buf).await;
        }

        // Differing values are bulk payload: claim pacing slots before allocating the snapshot.
        if !differences.is_empty() {
            debug!("returning {} diff_ranges", differences.len());
            trace!("diff_ranges: {differences:?}");
            if let Some((peer_guard, global_guard)) = self.try_claim_dump_slot(peer) {
                let updates: Vec<Message<K, Entry<Timestamp, V>, State<V>>> = {
                    let guard = self.map.load_full();
                    let mut updates = Vec::new();
                    for range in differences {
                        for (k, v) in guard.range(range) {
                            updates.push(Message::EntryUpdate((k.clone(), v.clone())));
                        }
                    }
                    updates
                };
                if !updates.is_empty() {
                    self.spawn_paced_send(
                        updates,
                        peer,
                        peer_guard,
                        global_guard,
                        DumpChannel::Dated,
                    );
                }
                // If updates is empty the guards drop here, releasing both slots.
            } else {
                self.stash_pending_dump(DumpChannel::Dated, peer, differences);
            }
        }

        if converged_with_nothing_to_send {
            trace!("comparison round from {peer} converged with nothing to send back; acking");
            send_messages_to(
                &[Message::ConvergenceAck::<K, Entry<Timestamp, V>, State<V>>],
                &self.send_ports(),
                &peer,
                send_buf,
            )
            .await;
        }
    }

    async fn apply_dated_updates(
        &self,
        updates: Vec<(K, Entry<Timestamp, V>)>,
        peer: SocketAddr,
        send_buf: &mut Vec<u8>,
    ) {
        if updates.is_empty() {
            return;
        }

        debug!("received {} updates", updates.len());
        observability::record_updates_received(updates.len());
        // A received bulk batch may have lost a sibling datagram, so schedule an early recheck and
        // suppress background re-initiation while this paced transfer may still be in progress.
        self.note_pending_repair(peer.ip());
        self.note_bulk_update_received(peer.ip());

        let mut acks_to_send = Vec::new();
        // Decide which remote values can change state under a read snapshot. Hooks deliberately
        // run only after this snapshot is released, so re-entrant writes cannot deadlock.
        let mut to_apply: Vec<(K, Entry<Timestamp, V>)> = Vec::new();
        {
            let guard = self.map.load_full();
            for (k, remote_v) in updates {
                // Advance the local HLC past every observed remote timestamp before any later
                // local write is minted.
                self.clock.observe(remote_v.stamp);
                match guard.get(&k) {
                    Some(local_v) => {
                        if remote_v.stamp > local_v.stamp {
                            to_apply.push((k, remote_v));
                        } else if local_v.is_tombstone() {
                            // Equal/newer local tombstones still need an acknowledgement.
                            acks_to_send.push(Message::TombstoneAck::<
                                K,
                                Entry<Timestamp, V>,
                                State<V>,
                            >((
                                k,
                                version_hash(local_v),
                            )));
                        }
                    }
                    None => to_apply.push((k, remote_v)),
                }
            }
        }

        for (k, v) in &to_apply {
            (self.pre_insert.read())(k, v);
        }

        // Reconcile again under the write lock: state may have changed while hooks ran.
        if !to_apply.is_empty() {
            self.record_changes(to_apply.len());
            let _guard = self.write_lock.lock();
            let mut map = (*self.map.load_full()).clone();
            let mut projection = (*self.projection.load_full()).clone();
            for (k, v) in to_apply {
                let merged_v = match map.get(&k) {
                    Some(local_v) => {
                        // Equal stamps with different content indicate a node-id collision; LWW
                        // cannot order that state, so report it instead of diverging silently.
                        if collision::is_node_id_collision(local_v, &v) {
                            self.collision_reporter.report(self.node_id());
                        }
                        local_v.merge(&v)
                    }
                    None => v,
                };
                let version = merged_v.is_tombstone().then(|| version_hash(&merged_v));
                self.map_insert(&mut map, &mut projection, k.clone(), merged_v);
                if let Some(version) = version {
                    acks_to_send.push(Message::TombstoneAck::<K, Entry<Timestamp, V>, State<V>>((
                        k, version,
                    )));
                }
            }
            self.map.store(Arc::new(map));
            self.projection.store(Arc::new(projection));
        }

        if !acks_to_send.is_empty() {
            send_messages_to(&acks_to_send, &self.send_ports(), &peer, send_buf).await;
        }
    }

    async fn handle_value_comparison(
        &self,
        value_in_comparison: Vec<RangeAggregate<K>>,
        peer: SocketAddr,
        send_buf: &mut Vec<u8>,
    ) {
        if value_in_comparison.is_empty() {
            return;
        }

        // Value-only reconciliation is independent of causal stability: dated stores answer from
        // the timestamp-less projection and never accept StateUpdate as authoritative input.
        debug!("received {} value-only segments", value_in_comparison.len());
        let mut differences = Vec::new();
        let mut out_comparison = Vec::new();
        {
            let guard = self.projection.load_full();
            let mut rng = self.rng.write();
            rbsr::protocol_round(
                &*guard,
                value_in_comparison,
                &mut out_comparison,
                &mut differences,
                &mut rng,
            );
        }

        if !out_comparison.is_empty() {
            let messages: Vec<_> = out_comparison
                .into_iter()
                .map(Message::StateFingerprint::<K, Entry<Timestamp, V>, State<V>>)
                .collect();
            send_messages_to(&messages, &self.send_ports(), &peer, send_buf).await;
        }

        if !differences.is_empty() {
            if let Some((peer_guard, global_guard)) = self.try_claim_dump_slot(peer) {
                let updates: Vec<Message<K, Entry<Timestamp, V>, State<V>>> = {
                    let guard = self.projection.load_full();
                    let mut updates = Vec::new();
                    for range in differences {
                        for (k, p) in guard.range(range) {
                            updates.push(Message::StateUpdate((k.clone(), p.clone())));
                        }
                    }
                    updates
                };
                if !updates.is_empty() {
                    self.spawn_paced_send(
                        updates,
                        peer,
                        peer_guard,
                        global_guard,
                        DumpChannel::ValueOnly,
                    );
                }
            } else {
                self.stash_pending_dump(DumpChannel::ValueOnly, peer, differences);
            }
        }
    }
}
