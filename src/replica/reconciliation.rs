// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ipnet::IpNet;
use rand::seq::SliceRandom;
use tracing::{instrument, trace, warn};

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::observability;

use super::{send_to_retry, version_hash, Message, Replica, TOMBSTONE_ACK_RESEND_BYTE_BUDGET};

impl<K: Key + Hash, V: Value> Replica<K, V> {
    #[instrument(name = "reconcile.round", skip_all)]
    pub async fn start_reconciliation(&self, send_buf: &mut Vec<u8>) {
        let timer = observability::timer();
        observability::record_reconcile_round();
        let segments = {
            let guard = self.map.load_full();
            rbsr::initial_ranges(&*guard)
        };
        send_buf.clear();
        for segment in segments {
            gossip::bincode::encode(
                &Message::EntryFingerprint::<K, Entry<Timestamp, V>, State<V>>(segment),
                send_buf,
            )
            .expect("serializing an EntryFingerprint into an in-memory buffer cannot fail");
        }
        // Snapshot the runtime-tunable topology once per round: no torn round, no lock held
        // across the sends below.
        let nets = self.nets.read().clone();
        let local = *self.local_net.read();
        let remote_interval = self.remote_interval.load(Ordering::Relaxed).max(1);
        let remote_fanout = self.remote_fanout.load(Ordering::Relaxed);
        let round = self.round.fetch_add(1, Ordering::Relaxed);
        *self.last_round_at.write() = Some(Instant::now());
        // Treat an interval of 0 as "every round" to avoid a modulo-by-zero.
        let do_remote = round % remote_interval == 0;
        let known = self.get_peers();

        // "What is the state now" gauges: refreshed once per round rather than at every
        // mutation — cheap at this cadence, and a gauge scraped periodically gains nothing from a
        // finer-grained update.
        let live_tombstones_len = self.live_tombstones.read().len();
        let total_entries = self.map.load_full().len();
        observability::record_state_gauges(
            known.len(),
            self.members.read().len(),
            live_entry_count(total_entries, live_tombstones_len),
            live_tombstones_len,
            self.bulk_dumps_in_flight.load(Ordering::Relaxed),
            self.broadcasts_in_flight.load(Ordering::Relaxed),
        );

        // De-duplicate so a discovery probe that happens to hit a known peer is not sent twice.
        let mut targets: HashSet<IpAddr> = HashSet::new();

        // Speculative probes only: an address that answers is registered then, not now. An
        // authoritative source drives the store's seed/decommission loop instead.
        targets.extend(self.probe.discover().await.unwrap_or_default());

        // Local network: contact every known peer, every round (fast intra-network convergence).
        for &addr in &known {
            if local.contains(&addr) {
                targets.insert(addr);
            }
        }

        // Remote peers on cross-network rounds only, a bounded subset per bucket, plus an
        // `unclassified` bucket: repair is decoupled from net membership, so a topology change
        // can never orphan a contacted peer from repair.
        if do_remote {
            let remote_nets: Vec<IpNet> = nets.iter().copied().filter(|&n| n != local).collect();
            let mut buckets: HashMap<Option<usize>, Vec<IpAddr>> = HashMap::new();
            for &addr in &known {
                if local.contains(&addr) {
                    continue; // already contacted every round above
                }
                let bucket = remote_nets.iter().position(|n| n.contains(&addr));
                buckets.entry(bucket).or_default().push(addr);
            }
            let mut rng = self.rng.write();
            for (_, mut peers) in buckets {
                peers.shuffle(&mut *rng);
                targets.extend(peers.into_iter().take(remote_fanout));
            }
        }

        // Piggyback causal-stability ack resends for the tombstones we hold.
        self.resend_held_tombstone_acks(send_buf);

        // Drop any target whose paced bulk transfer to us might still legitimately be in
        // progress -- re-initiating a full comparison mid-transfer only re-diffs and re-sends
        // ranges it is already sending, doubling traffic instead of
        // converging any faster. See `receiving_bulk_from`'s docs.
        let repair_interval = *self.repair_interval.read();
        let now = Instant::now();
        {
            let receiving = self.receiving_bulk_from.read();
            targets.retain(|addr| {
                !still_receiving_bulk(receiving.get(addr).copied(), now, repair_interval)
            });
        }

        // initiate the reconciliation protocol with the selected peers and discovery probes
        for peer in targets {
            trace!("initial_ranges {} bytes to {peer}", send_buf.len());
            if let Err(err) = send_to_retry(
                &*self.transport,
                &self.authenticator,
                &self.sender_counter,
                send_buf,
                SocketAddr::new(peer, self.port),
            )
            .await
            {
                warn!("failed to send reconciliation initiation to {peer}: {err}; continuing");
            }
            // If this round finds a real difference, `peer`'s reply (a SPLIT child, an
            // `EntryUpdate` batch) clears this the moment it arrives (`run`'s receive loop). A
            // round that resolves to a pure SKIP gets an explicit `ConvergenceAck` instead; see
            // `Message`'s docs.
            self.note_pending_repair(peer);
        }
        observability::record_round_duration(timer);
    }

    /// Append an ack for each held tombstone to `send_buf`, returning the count.
    ///
    /// Acks are otherwise pairwise, so past two nodes
    /// [`is_tombstone_stable`](Self::is_tombstone_stable) never completes; resending every round
    /// makes the matrix converge transitively, and makes an ack that arrived before its tombstone
    /// (dropped by the admission gate) recoverable on a later round.
    ///
    /// Bounded to [`TOMBSTONE_ACK_RESEND_BYTE_BUDGET`] bytes per datagram. When one datagram
    /// cannot cover every held tombstone, a key cursor resumes at the first unvisited key on the
    /// next round. If that key disappears meanwhile, ordered lookup resumes at its successor;
    /// wrapping eventually covers keys inserted behind the cursor.
    pub(super) fn resend_held_tombstone_acks(&self, send_buf: &mut Vec<u8>) -> usize {
        let mut keys: Vec<K> = self.live_tombstones.read().iter().cloned().collect();
        if keys.is_empty() {
            *self.tombstone_ack_resend_cursor.lock() = None;
            return 0;
        }
        keys.sort_unstable();
        let n = keys.len();
        let budget = send_buf.len() + TOMBSTONE_ACK_RESEND_BYTE_BUDGET;
        let mut cursor = self.tombstone_ack_resend_cursor.lock();
        let start = cursor
            .as_ref()
            .map_or(0, |next| match keys.binary_search(next) {
                Ok(index) => index,
                Err(index) if index < n => index,
                Err(_) => 0,
            });
        let map_guard = self.map.load_full();
        let mut appended = 0;
        let mut next_cursor = None;
        // Rotated, not `(start + offset) % n`-indexed: the cursor is a key, so a live-set change
        // can only move the next start to that key's ordered successor, never to an unrelated
        // numeric index in the resized set.
        for key in keys[start..].iter().chain(keys[..start].iter()) {
            if send_buf.len() >= budget {
                next_cursor = Some(key.clone());
                break;
            }
            // Re-confirm against the map: the tombstone may have been resurrected or GC'd since
            // we snapshotted the index, and only the live tombstone's version is a valid ack.
            if let Some(v) = map_guard.get(key).filter(|v| v.is_tombstone()) {
                gossip::bincode::encode(
                    &Message::TombstoneAck::<K, Entry<Timestamp, V>, State<V>>((
                        key.clone(),
                        version_hash(v),
                    )),
                    send_buf,
                )
                .expect("serializing a TombstoneAck into an in-memory buffer cannot fail");
                appended += 1;
            }
        }
        *cursor = next_cursor;
        if cursor.is_some() {
            trace!(
                "resent {appended}/{n} held-tombstone acks this round (datagram byte budget \
                 reached); the next round resumes at the first uncovered key"
            );
        }
        observability::record_tombstone_acks_resent(appended);
        appended
    }

    /// Record that a dated bulk-update batch just arrived from `peer` — see
    /// [`receiving_bulk_from`](super::Inner::receiving_bulk_from).
    pub(super) fn note_bulk_update_received(&self, peer: IpAddr) {
        self.receiving_bulk_from
            .write()
            .insert(peer, Instant::now());
    }
}

/// Live (non-tombstone) entry count for [`observability::record_state_gauges`]'s
/// `reconcile_entries_current` gauge, from the map's total size and the tombstone-index length.
///
/// `saturating_sub`, not `-`: the two counts come from separate, unsynchronized reads (`map`'s
/// `ArcSwap` and `live_tombstones`' `RwLock`), so a write landing between them can transiently
/// make `tombstones` outrun `total` — this must degrade to `0`, never underflow into a
/// near-`usize::MAX` gauge value or panic.
fn live_entry_count(total_entries: usize, tombstones: usize) -> usize {
    total_entries.saturating_sub(tombstones)
}

/// Whether a peer's most recently received dated bulk-update batch is recent enough that
/// [`start_reconciliation`](Replica::start_reconciliation) should leave it out of this round's
/// targets — the receiver-side guard against re-initiating a full diff mid-transfer. `None`
/// (never received one, or its entry was never set) is never
/// "still receiving".
fn still_receiving_bulk(
    last_received: Option<Instant>,
    now: Instant,
    repair_interval: Duration,
) -> bool {
    last_received.is_some_and(|last| now.saturating_duration_since(last) < repair_interval)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{live_entry_count, still_receiving_bulk};

    #[test]
    fn live_entry_count_subtracts_tombstones_from_the_total() {
        assert_eq!(live_entry_count(10, 3), 7);
    }

    #[test]
    fn live_entry_count_saturates_instead_of_underflowing() {
        assert_eq!(
            live_entry_count(3, 10),
            0,
            "tombstones observed as exceeding the total (a transient race between the two \
             unsynchronized reads) must saturate to 0, never underflow"
        );
    }

    #[test]
    fn no_bulk_update_ever_received_is_never_still_receiving() {
        assert!(!still_receiving_bulk(
            None,
            Instant::now(),
            Duration::from_millis(150)
        ));
    }

    #[test]
    fn a_bulk_update_received_well_within_repair_interval_is_still_receiving() {
        let now = Instant::now();
        let last = now.checked_sub(Duration::from_millis(10)).unwrap();
        assert!(still_receiving_bulk(
            Some(last),
            now,
            Duration::from_millis(150)
        ));
    }

    #[test]
    fn a_bulk_update_received_at_exactly_repair_interval_ago_is_no_longer_still_receiving() {
        let now = Instant::now();
        let last = now.checked_sub(Duration::from_millis(150)).unwrap();
        assert!(
            !still_receiving_bulk(Some(last), now, Duration::from_millis(150)),
            "the boundary itself must count as expired -- a strict `<`, not `<=`, so a grace \
             window can never be extended by re-arriving exactly on its own deadline"
        );
    }

    #[test]
    fn a_bulk_update_received_past_repair_interval_ago_is_no_longer_still_receiving() {
        let now = Instant::now();
        let last = now.checked_sub(Duration::from_millis(200)).unwrap();
        assert!(!still_receiving_bulk(
            Some(last),
            now,
            Duration::from_millis(150)
        ));
    }
}
