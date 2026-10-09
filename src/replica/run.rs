// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::time::Instant;

use tokio::time::timeout;
use tracing::{debug, instrument, trace, warn};

use crate::bounds::{Key, Value};
use crate::framing::{accept_frame, expire_reassembly, FrameEvent};
use crate::observability;

use super::{
    admit_inbound, complete_recovery, expire_recovery_state, retransmit_missing_to,
    send_recovery_control_to, InboundRejection, Replica, BUFFER_SIZE,
};

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Drive the gossip and reconciliation loops forever.
    ///
    /// This method does not return and cannot fail: network send errors are logged and
    /// counted, never fatal, so a vanished or unreachable peer cannot stop the loops.
    #[instrument(name = "reconcile.run", skip_all, fields(port = self.port))]
    pub async fn run(self) {
        let repair = self.clone();
        let recovery = self.clone();
        tokio::join!(
            self.recv_loop(),
            repair.repair_periodically(),
            recovery.retry_incomplete_periodically()
        );
    }

    async fn retry_incomplete_periodically(&self) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(3));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            super::recovery_send::retry_idle_incomplete(
                &self.send_ports(),
                &self.reassembler,
                self.port,
            )
            .await;
        }
    }

    /// The gossip receive loop: authenticate, dispatch, and re-initiate reconciliation on idle.
    /// Runs forever, alongside [`repair_periodically`](Self::repair_periodically) — see
    /// [`run`](Self::run).
    async fn recv_loop(&self) {
        // One byte larger than the largest legal datagram, so a message that fills it exactly
        // is distinguishable from one that was truncated.
        let mut recv_buf = [0; BUFFER_SIZE + 1];
        let mut send_buf = Vec::new();
        self.start_reconciliation(&mut send_buf).await;
        loop {
            // Re-read each iteration so the cadence can be retuned at runtime.
            let recv_timeout = *self.reconcile_interval.read();
            match timeout(recv_timeout, self.transport.recv_from(&mut recv_buf)).await {
                Err(_) => {
                    expire_reassembly(&self.reassembler);
                    expire_recovery_state(&self.send_ports());
                    debug!("no recent activity; initiating diff protocol");
                    self.start_reconciliation(&mut send_buf).await;
                }
                Ok(Err(err)) => {
                    warn!("network error in recv_from: {err}");
                    observability::record_datagram_dropped("recv_error");
                }
                Ok(Ok((size, peer))) => {
                    observability::record_bytes_received(size);
                    if peer.port() != self.port {
                        warn!(
                            "received message from {peer}, but protocol port is {}",
                            self.port
                        );
                    }
                    if size == recv_buf.len() {
                        warn!("Buffer too small for message, discarded");
                        observability::record_datagram_dropped("too_large");
                    } else {
                        let sender = peer.ip();
                        let payload = match admit_inbound(
                            &self.authenticator,
                            &self.replay_filter,
                            self.max_peers,
                            sender,
                            &recv_buf[..size],
                            || {
                                let guard = self.members.read();
                                (guard.contains(&sender), guard.len())
                            },
                        ) {
                            Ok(payload) => payload,
                            Err(InboundRejection::Authentication) => {
                                trace!("dropped datagram from {peer}: missing or invalid MAC");
                                observability::record_datagram_dropped("bad_mac");
                                continue;
                            }
                            Err(InboundRejection::Version(version)) => {
                                trace!(
                                    "dropped datagram from {peer}: wire version {version} != {}",
                                    gossip::auth::WIRE_VERSION
                                );
                                observability::record_datagram_dropped("version");
                                continue;
                            }
                            Err(InboundRejection::PeerCap { current_len, max }) => {
                                trace!(
                                    "dropped datagram from {peer}: peer cap reached \
                                     ({current_len}/{max})"
                                );
                                observability::record_datagram_dropped("peer_cap");
                                continue;
                            }
                            Err(InboundRejection::Replay { seq, stamp }) => {
                                trace!(
                                    "dropped replayed, stale, or replay-capacity datagram from \
                                     {peer}: seq={seq} stamp={stamp}"
                                );
                                observability::record_datagram_dropped("replay");
                                continue;
                            }
                        };
                        let max_missing_ranges = gossip::framing::max_missing_ranges_for_budget(
                            self.framing.datagram_payload_budget,
                            self.authenticator.overhead(),
                            self.framing.max_missing_ranges_per_report,
                        );
                        let payload = match accept_frame(
                            &self.reassembler,
                            sender,
                            payload,
                            max_missing_ranges,
                        ) {
                            FrameEvent::Logical {
                                payload,
                                completed_transfer,
                            } => {
                                if let Some(transfer_id) = completed_transfer {
                                    let supported = self.recovery.lock().supports(
                                        sender,
                                        Instant::now(),
                                        self.framing,
                                    );
                                    if supported {
                                        let mut frame = Vec::new();
                                        gossip::framing::write_completion_ack(
                                            transfer_id,
                                            &mut frame,
                                        );
                                        let _ = send_recovery_control_to(
                                            &self.send_ports(),
                                            peer,
                                            &frame,
                                            None,
                                        )
                                        .await;
                                    }
                                }
                                payload
                            }
                            FrameEvent::RequestMissing {
                                transfer_id,
                                ranges,
                            } => {
                                let supported = self.recovery.lock().supports(
                                    sender,
                                    Instant::now(),
                                    self.framing,
                                );
                                if supported {
                                    let mut frame = Vec::new();
                                    if gossip::framing::write_missing_report(
                                        transfer_id,
                                        &ranges,
                                        &mut frame,
                                    )
                                    .is_ok()
                                    {
                                        let _ = send_recovery_control_to(
                                            &self.send_ports(),
                                            peer,
                                            &frame,
                                            Some(ranges.len()),
                                        )
                                        .await;
                                    }
                                } else {
                                    observability::record_selective_recovery_fallback(
                                        "unsupported_peer",
                                    );
                                }
                                continue;
                            }
                            FrameEvent::Missing {
                                transfer_id,
                                ranges,
                            } => {
                                retransmit_missing_to(
                                    &self.send_ports(),
                                    peer,
                                    transfer_id,
                                    &ranges,
                                )
                                .await;
                                continue;
                            }
                            FrameEvent::CompleteAck { transfer_id } => {
                                complete_recovery(&self.send_ports(), peer, transfer_id);
                                continue;
                            }
                            FrameEvent::Pending => continue,
                        };
                        // Only a complete logical payload proves the preceding exchange made
                        // protocol progress. Incomplete fragments leave the RTT-scale repair
                        // pending so a lost fragment is retried with the same content address.
                        self.pending_repairs.write().remove(&sender);
                        let spoke_dated = self.handle_messages(payload, peer, &mut send_buf).await;
                        // Only a sender that spoke the dated channel joins causal-stability
                        // membership; value-only read replicas never gate tombstone GC.
                        if spoke_dated {
                            self.peers.write().insert(sender, Instant::now());
                            let mut generation = self.snapshot_generations.mutation();
                            if self.members.write().insert(sender) {
                                generation.record_member(sender, true);
                                self.record_changes(1);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::replica::PeerCap;

    /// `max()` reflects the constructed value — its only call site today is a `trace!` format
    /// string, so nothing else in the crate would catch a mutant hardcoding a constant return.
    #[test]
    fn peer_cap_max_reflects_the_constructed_value() {
        assert_eq!(PeerCap::new(5).max(), 5);
    }
}
