// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! RTT-scale repair for a lost comparison-round, ack, or bulk-transfer datagram (#23).
//!
//! Before this, the only thing that ever re-issued a comparison round was
//! [`start_reconciliation`](super::Replica::start_reconciliation)'s own idle timeout —
//! `reconcile_interval`, seconds by default. A datagram dropped in flight was therefore repaired
//! on that cadence, not on RTT, however small the actual network loss. This tracks, per peer, a
//! comparison round this node is owed an answer to (or wants a fresh one for, after a bulk
//! transfer) and retries it on the much shorter [`repair_interval`](super::Inner::repair_interval)
//! instead — cleared the instant *any* reply comes back from that peer ([`run`](super::Replica::run)):
//! a real difference, or, for a round that converges with nothing else to say,
//! [`Message::ConvergenceAck`](super::Message::ConvergenceAck). What still falls back to a bounded
//! retry is a datagram — the original round or its ack — genuinely lost in flight, not a
//! converged round going unacknowledged by protocol design: small and bounded either way, the
//! same order of magnitude as the existing per-round tombstone-ack resend, not the bulk-transfer
//! amplification akvize/reconcile-rs#168/#177 fixed.

use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use tracing::trace;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};

use super::{send_to_retry, Message, Replica};

/// Bound on how many times one outstanding comparison round is retried before this node gives up
/// on it and leaves the peer to the next full background round instead — a dead or partitioned
/// peer must not be retried forever at [`repair_interval`](super::Inner::repair_interval).
pub(super) const MAX_REPAIR_ATTEMPTS: u32 = 4;

/// One peer this node owes (or is owed a reply for) a fresh comparison round — see
/// [`pending_repairs`](super::Inner::pending_repairs).
pub(super) struct PendingRepair {
    /// When this entry is next due for a retry.
    deadline: Instant,
    /// How many retries have already been sent for this entry.
    attempts: u32,
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Mark `peer` as owed a fresh comparison round within
    /// [`repair_interval`](super::Inner::repair_interval), unless one is already pending — an
    /// already-scheduled retry keeps its existing deadline rather than being pushed back by a
    /// second, unrelated reason to check in on the same peer.
    pub(super) fn note_pending_repair(&self, peer: IpAddr) {
        let deadline = Instant::now() + *self.repair_interval.read();
        self.pending_repairs
            .write()
            .entry(peer)
            .or_insert(PendingRepair {
                deadline,
                attempts: 0,
            });
    }

    /// Send one freshly computed full-store [`Message::EntryFingerprint`] to `peer` —
    /// [`start_reconciliation`](Self::start_reconciliation)'s per-target send, scoped to a single
    /// peer and without its topology targeting or tombstone-ack piggybacking, for a narrowly
    /// targeted repair retry.
    async fn send_comparison_to(&self, peer: IpAddr, send_buf: &mut Vec<u8>) {
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
        let target = SocketAddr::new(peer, self.port);
        if let Err(err) = send_to_retry(
            &*self.transport,
            &self.authenticator,
            &self.sender_counter,
            send_buf,
            target,
        )
        .await
        {
            trace!("failed to send repair comparison to {peer}: {err}; continuing");
        }
    }

    /// Periodically retry any comparison round that has gone unanswered for
    /// [`repair_interval`](super::Inner::repair_interval) — the mechanism that decouples loss
    /// repair from `reconcile_interval`'s background cadence (#23). Runs forever; driven
    /// alongside the receive loop by [`run`](Self::run).
    pub(super) async fn repair_periodically(&self) {
        let mut send_buf = Vec::new();
        loop {
            let interval = *self.repair_interval.read();
            tokio::time::sleep(interval).await;
            self.retry_due_repairs(&mut send_buf).await;
        }
    }

    async fn retry_due_repairs(&self, send_buf: &mut Vec<u8>) {
        let now = Instant::now();
        let interval = *self.repair_interval.read();
        let due: Vec<IpAddr> = {
            let mut guard = self.pending_repairs.write();
            let mut due = Vec::new();
            guard.retain(|&peer, repair| {
                if repair.deadline > now {
                    return true; // not due yet, keep waiting
                }
                repair.attempts += 1;
                if repair.attempts > MAX_REPAIR_ATTEMPTS {
                    trace!(
                        "giving up repairing the comparison round with {peer} after \
                         {MAX_REPAIR_ATTEMPTS} unanswered retries; the next background round \
                         will pick it back up"
                    );
                    return false;
                }
                repair.deadline = now + interval;
                due.push(peer);
                true
            });
            due
        };
        for peer in due {
            trace!("retrying unanswered comparison round with {peer}");
            self.send_comparison_to(peer, send_buf).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::clock::{ManualClock, NodeId};
    use crate::replica::Replica;
    use crate::replicated_map::Config;
    use crate::transport::InMemoryNetwork;

    fn engine(ip: IpAddr, port: u16, net: &InMemoryNetwork) -> Replica<u32, u32> {
        let addr = std::net::SocketAddr::new(ip, port);
        Replica::new_with_transport(
            Config::default()
                .with_listen_addr(ip)
                .with_port(port)
                .with_insecure_no_key(),
            Arc::new(net.bind(addr)),
            Arc::new(ManualClock::new(NodeId::new(1))),
        )
    }

    /// A freshly noted repair is not retried before its deadline: `retry_due_repairs` must not
    /// fire early just because it happened to run.
    #[tokio::test]
    async fn a_fresh_pending_repair_is_not_yet_due() {
        let net = InMemoryNetwork::new();
        let port = crate::replica::tests::next_ephemeral_test_port();
        let a: Replica<u32, u32> = engine("127.0.0.10".parse().unwrap(), port, &net);
        let peer: IpAddr = "127.0.0.11".parse().unwrap();
        a.set_repair_interval(Duration::from_secs(3600));
        a.note_pending_repair(peer);
        assert_eq!(
            a.pending_repairs.read().len(),
            1,
            "note_pending_repair must record the peer"
        );
        a.retry_due_repairs(&mut Vec::new()).await;
        let guard = a.pending_repairs.read();
        let entry = guard.get(&peer);
        assert!(
            entry.is_some(),
            "a repair scheduled 3600s out must still be pending immediately afterward, not \
             retried early"
        );
        assert_eq!(
            entry.unwrap().attempts,
            0,
            "a repair not yet due must not count as an attempt -- distinguishes \"not due yet\" \
             from \"due, and this was its first retry\""
        );
    }

    /// After a retry fires, the entry's deadline must move *forward* by `repair_interval` --
    /// not backward, which would make it immediately due again on the very next check instead
    /// of waiting a full interval.
    #[tokio::test]
    async fn a_retried_repair_is_not_immediately_due_again() {
        let net = InMemoryNetwork::new();
        let port = crate::replica::tests::next_ephemeral_test_port();
        let a: Replica<u32, u32> = engine("127.0.0.16".parse().unwrap(), port, &net);
        let peer: IpAddr = "127.0.0.17".parse().unwrap();
        a.set_repair_interval(Duration::from_millis(20));
        a.note_pending_repair(peer);
        tokio::time::sleep(Duration::from_millis(25)).await;
        a.retry_due_repairs(&mut Vec::new()).await;
        assert_eq!(
            a.pending_repairs.read().get(&peer).unwrap().attempts,
            1,
            "the first retry, once due, must actually fire"
        );
        // No sleep here: a correctly-forward-moved deadline is not due yet, so this immediate
        // second check must not count as another retry.
        a.retry_due_repairs(&mut Vec::new()).await;
        assert_eq!(
            a.pending_repairs.read().get(&peer).unwrap().attempts,
            1,
            "a repair just retried must not be immediately due again -- its deadline should \
             have moved a full repair_interval into the future, not into the past"
        );
    }

    /// A second `note_pending_repair` for the same peer while one is already pending must not
    /// push its deadline back out — otherwise a peer that keeps giving new reasons to check in
    /// (e.g. repeated bulk updates) could have its repair indefinitely deferred.
    #[tokio::test]
    async fn a_second_note_does_not_reset_an_already_pending_deadline() {
        let net = InMemoryNetwork::new();
        let port = crate::replica::tests::next_ephemeral_test_port();
        let a: Replica<u32, u32> = engine("127.0.0.12".parse().unwrap(), port, &net);
        let peer: IpAddr = "127.0.0.13".parse().unwrap();
        a.set_repair_interval(Duration::from_millis(20));
        a.note_pending_repair(peer);
        let first_deadline = a.pending_repairs.read().get(&peer).unwrap().deadline;
        tokio::time::sleep(Duration::from_millis(5)).await;
        a.note_pending_repair(peer);
        let second_deadline = a.pending_repairs.read().get(&peer).unwrap().deadline;
        assert_eq!(
            first_deadline, second_deadline,
            "an already-pending repair's deadline must not move when noted again"
        );
    }

    /// Past `MAX_REPAIR_ATTEMPTS` unanswered retries, the entry is dropped rather than retried
    /// forever — a partitioned or dead peer must not be pinged at `repair_interval` indefinitely.
    /// Pins the exact boundary (still present at exactly `MAX_REPAIR_ATTEMPTS`, gone one retry
    /// later) rather than just "eventually gone", which alone cannot tell a bound that gives up
    /// one attempt early or late from the real one.
    #[tokio::test]
    async fn a_repair_is_dropped_after_the_attempt_bound() {
        let net = InMemoryNetwork::new();
        let port = crate::replica::tests::next_ephemeral_test_port();
        let a: Replica<u32, u32> = engine("127.0.0.14".parse().unwrap(), port, &net);
        let peer: IpAddr = "127.0.0.15".parse().unwrap();
        a.set_repair_interval(Duration::from_millis(1));
        a.note_pending_repair(peer);
        for _ in 0..super::MAX_REPAIR_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(2)).await;
            a.retry_due_repairs(&mut Vec::new()).await;
        }
        {
            let guard = a.pending_repairs.read();
            let entry = guard.get(&peer);
            assert!(
                entry.is_some(),
                "a repair must still be pending right at {} attempts, not given up on early",
                super::MAX_REPAIR_ATTEMPTS
            );
            assert_eq!(
                entry.unwrap().attempts,
                super::MAX_REPAIR_ATTEMPTS,
                "exactly {} attempts must have been recorded by this point",
                super::MAX_REPAIR_ATTEMPTS
            );
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
        a.retry_due_repairs(&mut Vec::new()).await;
        assert!(
            a.pending_repairs.read().get(&peer).is_none(),
            "a repair must be given up on after {} unanswered attempts, not retried forever",
            super::MAX_REPAIR_ATTEMPTS
        );
    }
}
