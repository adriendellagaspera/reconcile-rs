// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::trace;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::observability;
use crate::FingerprintTreeMap;

use super::{send_messages_to, Message, Replica, SendPorts, PEER_EXPIRATION};

/// Broadcast-egress budget was exhausted when [`Replica::try_insert`] attempted to claim a slot
/// (#83). Crate-internal signal carrying no state of its own — the public
/// [`Backpressure`](crate::replicated_map::Backpressure) error
/// [`ReplicatedMap::try_insert`](crate::replicated_map::ReplicatedMap::try_insert)/
/// [`try_update`](crate::replicated_map::ReplicatedMap::try_update) build from it reads the
/// in-flight/budget counts fresh through [`Replica::broadcasts_in_flight`]/
/// [`Replica::max_concurrent_broadcasts`] rather than snapshotting them here, since the reject and
/// the read are not atomic with each other anyway.
pub(crate) struct BroadcastBudgetExhausted;

/// RAII counter-decrement for the global concurrent-broadcast budget (#83). Mirrors
/// [`pacing::BulkDumpCountGuard`](super::pacing::BulkDumpCountGuard): decrements the shared atomic
/// on `Drop`, so a panicking or aborted send task cannot wedge the budget below its true capacity.
pub(crate) struct BroadcastCountGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for BroadcastCountGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Release);
    }
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Insert into an already-loaded, already-owned clone of the dated `map` **and** mirror the
    /// value-only projection (and the live-tombstone index). The caller must hold
    /// [`write_lock`](super::Inner::write_lock) and `store()` both `map`/`projection` back once it
    /// is done — this only mutates the two owned clones handed to it, so several calls can share
    /// one load/store pair (see [`just_insert_bulk`](Self::just_insert_bulk)).
    pub(super) fn map_insert(
        &self,
        map: &mut FingerprintTreeMap<K, Entry<Timestamp, V>>,
        projection: &mut FingerprintTreeMap<K, State<V>>,
        key: K,
        value: Entry<Timestamp, V>,
    ) -> Option<Entry<Timestamp, V>> {
        // Keep the live-tombstone index in step with the map at its single mutation sink: a
        // tombstone value adds the key; any live value (a fresh insert, or an LWW overwrite that
        // resurrects a previously-deleted key) removes it. This index drives the per-round
        // causal-stability ack resend in `start_reconciliation`.
        {
            let mut live_tombstones = self.live_tombstones.write();
            if value.is_tombstone() {
                live_tombstones.insert(key.clone());
            } else {
                live_tombstones.remove(&key);
            }
        }
        projection.insert(key.clone(), value.project());
        map.insert(key, value)
    }

    pub(super) fn get_peers(&self) -> Vec<IpAddr> {
        let mut guard = self.peers.write();
        guard.retain(|_, instant| instant.elapsed() < PEER_EXPIRATION);
        guard.keys().cloned().collect()
    }

    pub fn just_insert(&self, key: K, value: Entry<Timestamp, V>) -> Option<Entry<Timestamp, V>> {
        // Hooks run outside the write lock: a hook that re-inserts must not re-enter it and
        // deadlock (matching the update-merge path in `handle_messages`).
        (self.pre_insert.read())(&key, &value);

        // A tombstone value is a removal; a live value is an insertion. Counting here (rather
        // than in `ReplicatedMap`) keeps every local mutation path covered.
        if value.is_tombstone() {
            observability::record_remove();
        } else {
            observability::record_insert();
        }
        self.record_changes(1);

        let _guard = self.write_lock.lock();
        let mut map = (*self.map.load_full()).clone();
        let mut projection = (*self.projection.load_full()).clone();
        let ret = self.map_insert(&mut map, &mut projection, key, value);
        self.map.store(Arc::new(map));
        self.projection.store(Arc::new(projection));
        ret
    }

    /// Claim one of the [`max_concurrent_broadcasts`](super::Inner::max_concurrent_broadcasts)
    /// egress slots, or `None` if the budget is exhausted (#83). Mirrors
    /// [`Replica::try_claim_dump_slot`]'s compare-exchange loop, without that one's per-peer half
    /// — a write-broadcast task fans out to every peer at once, there is no per-peer slot to also
    /// hold.
    ///
    /// `pub(crate)` (not private): `ReplicatedMap::try_update` (`replicated_map/mutate.rs`) claims
    /// a slot before deciding whether the key is even live, matching [`try_insert`](Self::try_insert)'s
    /// all-or-nothing ordering there too, and then hands the claimed guard to
    /// [`broadcast_update_with_claimed_slot`](Self::broadcast_update_with_claimed_slot).
    pub(crate) fn try_claim_broadcast_slot(&self) -> Option<BroadcastCountGuard> {
        let budget = self.max_concurrent_broadcasts;
        self.broadcasts_in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n < budget {
                    Some(n + 1)
                } else {
                    None
                }
            })
            .ok()?;
        Some(BroadcastCountGuard {
            counter: Arc::clone(&self.broadcasts_in_flight),
        })
    }

    /// Write-broadcast tasks currently in flight — the depth gauge #83 asks for. Also backs
    /// [`Backpressure`](crate::replicated_map::Backpressure)'s `in_flight` field.
    pub(crate) fn broadcasts_in_flight(&self) -> usize {
        self.broadcasts_in_flight.load(Ordering::Acquire)
    }

    /// The configured [`max_concurrent_broadcasts`](super::Inner::max_concurrent_broadcasts)
    /// budget. Backs [`Backpressure`](crate::replicated_map::Backpressure)'s `max_in_flight`
    /// field.
    pub(crate) fn max_concurrent_broadcasts(&self) -> usize {
        self.max_concurrent_broadcasts
    }

    /// Broadcast a batch of messages to every known peer, on a detached task so the write path
    /// does not block on the network. The low-level send primitive both the immediate path and
    /// the coalescing flush ([`queue_broadcast`](Self::queue_broadcast)) reduce to.
    ///
    /// Bounded by [`max_concurrent_broadcasts`](super::Inner::max_concurrent_broadcasts) (#83): at
    /// the budget, this skips spawning the task entirely rather than queuing it — the write this
    /// call follows has already applied locally, so nothing is lost, only the eager push is
    /// delayed to the next [`reconcile_interval`](super::Inner::reconcile_interval) round or
    /// [`repair_interval`](super::Inner::repair_interval) retry (#23), exactly the bounded cost an
    /// already-tolerated lost datagram is. A caller that wants to *know* egress is falling behind
    /// rather than rely on that backstop uses [`try_insert`](Self::try_insert) instead, which
    /// rejects the whole call up front.
    pub(super) fn broadcast(&self, messages: Vec<Message<K, Entry<Timestamp, V>, State<V>>>) {
        let Some(guard) = self.try_claim_broadcast_slot() else {
            observability::record_broadcast_backpressure("eager");
            trace!(
                "skipped write-broadcast: egress budget ({}) exhausted; the write itself already \
                 applied, next reconciliation round recovers it",
                self.max_concurrent_broadcasts
            );
            return;
        };
        self.spawn_broadcast(messages, guard);
    }

    /// The actual detached send task, factored out of [`broadcast`](Self::broadcast) so
    /// [`try_insert`](Self::try_insert) can spawn it too, over a slot it already claimed itself.
    fn spawn_broadcast(
        &self,
        messages: Vec<Message<K, Entry<Timestamp, V>, State<V>>>,
        guard: BroadcastCountGuard,
    ) {
        let peers = self.get_peers();
        let port = self.port;
        let transport = Arc::clone(&self.transport);
        let authenticator = self.authenticator.clone();
        let sender_counter = Arc::clone(&self.sender_counter);
        // #23: a plain `Update` triggers no reply of its own, so this specific write cannot
        // itself cancel the pending retry early -- only unrelated traffic from the same peer, or
        // the bounded timeout in `retry_due_repairs`, resolves it. An accepted, bounded cost (see
        // `Message`'s docs), not a bug. Cloning the whole engine (an `Arc` bump) rather than more
        // individual fields, since this is the one thing here that needs `Replica`'s own methods
        // rather than just the transport/auth.
        let repair_engine = self.clone();
        tokio::spawn(async move {
            // Held for the task's lifetime, releasing the egress slot on completion, panic, or
            // abort alike.
            let _guard = guard;
            let ports = SendPorts {
                transport: &*transport,
                authenticator: &authenticator,
                sender_counter: &sender_counter,
            };
            let mut send_buf = Vec::new();
            for addr in peers {
                let peer = SocketAddr::new(addr, port);
                send_messages_to(&messages, &ports, &peer, &mut send_buf).await;
                // Watch for a reply within `repair_interval`; a lost broadcast would otherwise
                // only be rediscovered by the next full `reconcile_interval` round (#23).
                repair_engine.note_pending_repair(addr);
            }
        });
    }

    pub fn insert(&self, key: K, value: Entry<Timestamp, V>) -> Option<Entry<Timestamp, V>> {
        let ret = self.just_insert(key.clone(), value.clone());
        self.queue_broadcast(vec![(key, value)]);
        ret
    }

    /// Fallible counterpart of [`insert`](Self::insert) (#83): claims a
    /// [`max_concurrent_broadcasts`](super::Inner::max_concurrent_broadcasts) egress slot
    /// **before** touching the map, so a call either fully applies — locally and broadcast — or
    /// not at all, never a write with a silently-skipped broadcast the way `insert` accepts.
    /// Always sends immediately, bypassing [`queue_broadcast`](Self::queue_broadcast)'s
    /// coalescing: a caller reaching for backpressure feedback wants to know now, not after
    /// `coalesce_window` elapses.
    ///
    /// # Errors
    ///
    /// [`BroadcastBudgetExhausted`] when the egress budget is already at capacity. The map is
    /// untouched — retry, buffer, or drop is the caller's call.
    pub fn try_insert(
        &self,
        key: K,
        value: Entry<Timestamp, V>,
    ) -> Result<Option<Entry<Timestamp, V>>, BroadcastBudgetExhausted> {
        let guard = self
            .try_claim_broadcast_slot()
            .ok_or(BroadcastBudgetExhausted)?;
        let ret = self.just_insert(key.clone(), value.clone());
        self.spawn_broadcast(vec![Message::EntryUpdate((key, value))], guard);
        Ok(ret)
    }

    /// As [`broadcast_update`](Self::broadcast_update), but over a slot the caller already
    /// claimed via [`try_claim_broadcast_slot`](Self::try_claim_broadcast_slot) —
    /// `ReplicatedMap::try_update`'s live branch. Bypasses coalescing for the same reason
    /// [`try_insert`](Self::try_insert) does.
    pub(crate) fn broadcast_update_with_claimed_slot(
        &self,
        key: K,
        value: Entry<Timestamp, V>,
        guard: BroadcastCountGuard,
    ) {
        self.spawn_broadcast(vec![Message::EntryUpdate((key, value))], guard);
    }

    /// Broadcast a single locally-mutated entry to peers, mirroring [`insert`](Self::insert)'s
    /// propagation. Used by in-place mutation paths (`ReplicatedMap::get_mut`) that write the map
    /// directly and must still notify peers so the edit reconciles, without re-applying it locally.
    pub(crate) fn broadcast_update(&self, key: K, value: Entry<Timestamp, V>) {
        self.queue_broadcast(vec![(key, value)]);
    }

    pub fn just_insert_bulk(&self, key_values: &[(K, Entry<Timestamp, V>)]) {
        // Hooks run outside the write lock, for the same re-entrancy reason as `just_insert`.
        for (key, value) in key_values {
            (self.pre_insert.read())(key, value);
            if value.is_tombstone() {
                observability::record_remove();
            } else {
                observability::record_insert();
            }
        }
        self.record_changes(key_values.len());
        let _guard = self.write_lock.lock();
        let mut map = (*self.map.load_full()).clone();
        let mut projection = (*self.projection.load_full()).clone();
        for (key, value) in key_values {
            self.map_insert(&mut map, &mut projection, key.clone(), value.clone());
        }
        self.map.store(Arc::new(map));
        self.projection.store(Arc::new(projection));
    }

    pub fn insert_bulk(&self, key_values: &[(K, Entry<Timestamp, V>)]) {
        self.just_insert_bulk(key_values);
        self.queue_broadcast(key_values.to_vec());
    }

    /// Count `n` more changes toward [`Config::snapshot_change_threshold`](crate::replicated_map::Config::snapshot_change_threshold)
    /// — see [`Inner::changes_since_snapshot`](super::Inner::changes_since_snapshot) for which
    /// mutation sinks call this and why. `n == 0` is a harmless no-op (`fetch_add(0, ..)` leaves
    /// the counter unchanged), so callers never need to guard the call themselves.
    pub(crate) fn record_changes(&self, n: usize) {
        self.changes_since_snapshot.fetch_add(n, Ordering::Relaxed);
    }

    /// Changes counted since the last successful snapshot (or since construction, if none yet).
    pub(crate) fn change_count(&self) -> usize {
        self.changes_since_snapshot.load(Ordering::Relaxed)
    }

    /// Zero the change counter — called only after a successful snapshot write
    /// (`replicated_map/persistence.rs`).
    pub(crate) fn reset_change_count(&self) {
        self.changes_since_snapshot.store(0, Ordering::Relaxed);
    }
}
