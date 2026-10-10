// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use gossip::runtime as tokio;
use std::net::SocketAddr;
use std::sync::{atomic::Ordering, Arc};
use web_time::Instant;

use tokio::time::timeout;
use tracing::{debug, trace, warn};

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::framing::{accept_frame, expire_reassembly, FrameEvent};
use crate::replica::{
    admit_inbound, append_capability, complete_recovery, expire_recovery_state,
    is_selective_recovery_capability, retransmit_missing_to, send_control_batch_to,
    send_recovery_control_to, send_to_retry, InboundRejection, Message, SendPorts,
    MAX_MESSAGES_PER_DATAGRAM,
};
use crate::transport::Transport;
use gossip::auth;
use gossip::framing::LogicalPayload;
use gossip::gen_ip::gen_ip;

use super::ReadReplicaMap;

const BUFFER_SIZE: usize = 65507;

/// The wire value type, named only so the shared [`Message`] enum has a concrete `Update` payload
/// — which a read replica ignores, storing no dated value.
type WireDated<V> = Entry<Timestamp, V>;

impl<K: Key, V: Value> ReadReplicaMap<K, V> {
    /// Set the hook invoked outside the map lock, before each inbound value is integrated. A
    /// deletion arrives as `State::Tombstone`. This is a setter: a second call replaces the
    /// first, it does not add to it.
    pub fn set_on_update<F: Send + Sync + Fn(&K, &State<V>) + 'static>(&self, on_update: F) {
        *self.on_update.write() = Box::new(on_update);
    }

    /// Integrate inbound value-only updates by plain overwrite (a read replica holds no timestamp to
    /// compare against — it trusts the authoritative dated peer). Hooks run outside the write
    /// lock, so a hook may safely call back into the read replica.
    pub(super) fn integrate(&self, updates: Vec<(K, State<V>)>) {
        if updates.is_empty() {
            return;
        }
        {
            let hook = self.on_update.read();
            for (k, state) in &updates {
                hook(k, state);
            }
        }
        let _guard = self.write_lock.lock();
        let mut tree = (*self.tree.load_full()).clone();
        for (k, state) in updates {
            tree.insert(k, state);
        }
        self.tree.store(Arc::new(tree));
    }

    /// Bundle the outbound ports the batched-send helpers need, exactly as
    /// [`Replica`](crate::Replica) does. See [`SendPorts`].
    fn send_ports(&self) -> SendPorts<'_, dyn Transport> {
        SendPorts {
            transport: &*self.transport,
            authenticator: &self.authenticator,
            sender_counter: &self.sender_counter,
            framing: self.framing,
            recovery: &self.recovery,
        }
    }

    /// Run one round of value-only anti-entropy against the configured peers. Normally driven by
    /// [`run`](Self::run)'s loop; exposed for callers that want to force an out-of-band round
    /// (e.g. in tests), mirroring [`ReplicatedMap::start_reconciliation`](crate::ReplicatedMap::start_reconciliation).
    pub async fn start_reconciliation(&self) {
        let mut send_buf = Vec::new();
        self.start_reconciliation_inner(&mut send_buf).await;
    }

    /// Send our value-only comparison items to every known peer plus a random address (discovery),
    /// kicking off / continuing a value-only reconciliation round. `send_buf` is caller-owned so
    /// [`run`](Self::run)'s hot loop can reuse one allocation across rounds.
    async fn start_reconciliation_inner(&self, send_buf: &mut Vec<u8>) {
        self.round.fetch_add(1, Ordering::Relaxed);
        *self.last_round_at.write() = Some(Instant::now());
        let segments = rbsr::initial_ranges(&*self.tree.load_full());
        send_buf.clear();
        for segment in segments {
            gossip::bincode::encode(
                &Message::StateFingerprint::<K, WireDated<V>, State<V>>(segment),
                send_buf,
            )
            .expect("serializing a StateFingerprint into an in-memory buffer cannot fail");
        }
        append_capability::<K, WireDated<V>, State<V>>(self.framing, send_buf);
        let mut peers = self.peers();
        // A random address out of the peer network, for discovery — like the dated store, we do not
        // add it to the known peers; a real peer there will answer and be recorded then.
        let net = *self.net.read();
        let addr = gen_ip(&mut *self.rng.write(), net);
        peers.push(addr);
        for peer in peers {
            trace!(
                "read replica initial_ranges {} bytes to {peer}",
                send_buf.len()
            );
            if let Err(err) = send_to_retry(
                &*self.transport,
                &self.authenticator,
                &self.sender_counter,
                self.framing,
                send_buf,
                SocketAddr::new(peer, self.port),
            )
            .await
            {
                warn!(
                    "read replica failed to send reconciliation initiation to {peer}: {err}; \
                     continuing"
                );
            }
        }
    }

    async fn handle_messages(
        &self,
        payload: LogicalPayload<'_>,
        peer: SocketAddr,
        send_buf: &mut Vec<u8>,
    ) {
        let payload = payload.as_bytes();
        trace!("read replica received {} bytes from {peer}", payload.len());
        let mut value_in_comparison = Vec::new();
        let mut value_updates: Vec<(K, State<V>)> = Vec::new();
        // `MAX_MESSAGES_PER_DATAGRAM` bounds the expansion; a malformed datagram is dropped whole.
        let messages: Vec<Message<K, WireDated<V>, State<V>>> =
            match gossip::bincode::decode_stream(payload, MAX_MESSAGES_PER_DATAGRAM) {
                Ok(messages) => messages,
                Err(kind) => {
                    warn!(
                        "read replica failed to deserialize datagram from {peer}, dropping it: \
                         {kind:?}"
                    );
                    return;
                }
            };
        for message in messages {
            match message {
                Message::StateFingerprint(segment) => value_in_comparison.push(segment),
                Message::StateUpdate(update) => value_updates.push(update),
                // The dated channel is meaningless to a read replica (it cannot store dated values
                // nor participate in causal stability, so it never sends an `EntryFingerprint`
                // this could even be an ack for). Ignore it, same as the other dated-only messages.
                Message::EntryFingerprint(_)
                | Message::EntryUpdate(_)
                | Message::TombstoneAck(_)
                | Message::ConvergenceAck => {}
                Message::Reserved6(payload) => {
                    if is_selective_recovery_capability(&payload) {
                        self.recovery.lock().record_capability(
                            peer.ip(),
                            Instant::now(),
                            self.framing,
                            self.max_peers.max(),
                        );
                    }
                }
            }
        }

        self.integrate(value_updates);

        if !value_in_comparison.is_empty() {
            debug!(
                "read replica received {} value-only segments",
                value_in_comparison.len()
            );
            let mut out_comparison = Vec::new();
            let mut differences = Vec::new();
            {
                let guard = self.tree.load_full();
                let mut rng = self.rng.write();
                rbsr::protocol_round(
                    &*guard,
                    value_in_comparison,
                    &mut out_comparison,
                    &mut differences,
                    &mut rng,
                );
            }
            // `differences` are ranges this read replica would owe the peer. A read-only replica
            // never sends authoritative values, so we deliberately drop them and only bounce back
            // the refined comparison items that keep the peer's side of the diff progressing.
            if !out_comparison.is_empty() {
                let messages: Vec<_> = out_comparison
                    .into_iter()
                    .map(Message::<K, WireDated<V>, State<V>>::StateFingerprint)
                    .collect();
                send_control_batch_to(&messages, &self.send_ports(), &peer, send_buf).await;
            }
        }
    }

    /// Run the read replica's reconciliation loop forever. Spawn this on a task; the read replica
    /// converges to the dated cluster's current values and reflects deletions as tombstones.
    /// Alongside it, drives the discovery task ([`with_discovery`](Self::with_discovery)) when one
    /// is configured — a no-op otherwise.
    pub async fn run(self) {
        tokio::join!(
            self.run_reconciliation_loop(),
            self.discover_periodically(),
            self.retry_incomplete_periodically(),
        );
    }

    async fn retry_incomplete_periodically(&self) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(3));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            crate::replica::retry_idle_incomplete(&self.send_ports(), &self.reassembler, self.port)
                .await;
        }
    }

    /// The reconciliation half of [`run`](Self::run), split out so it can run concurrently with
    /// [`discover_periodically`](Self::discover_periodically).
    async fn run_reconciliation_loop(&self) {
        let mut recv_buf = [0; BUFFER_SIZE + 1];
        let mut send_buf = Vec::new();
        self.start_reconciliation_inner(&mut send_buf).await;
        loop {
            let activity_timeout = *self.reconcile_interval.read();
            match timeout(activity_timeout, self.transport.recv_from(&mut recv_buf)).await {
                Err(_) => {
                    expire_reassembly(&self.reassembler);
                    expire_recovery_state(&self.send_ports());
                    debug!("read replica: no recent activity; initiating value-only diff");
                    self.start_reconciliation_inner(&mut send_buf).await;
                }
                Ok(Err(err)) => warn!("read replica network error in recv_from: {err}"),
                Ok(Ok((size, peer))) => {
                    if peer.port() != self.port {
                        warn!(
                            "read replica received message from {peer}, but protocol port is {}",
                            self.port
                        );
                    }
                    if size == recv_buf.len() {
                        warn!("read replica buffer too small for message, discarded");
                    } else {
                        let sender = peer.ip();
                        let payload = match admit_inbound(
                            &self.authenticator,
                            &self.replay_filter,
                            self.max_peers,
                            sender,
                            &recv_buf[..size],
                            || {
                                let guard = self.peers.read();
                                (guard.contains_key(&sender), guard.len())
                            },
                        ) {
                            Ok(payload) => payload,
                            Err(InboundRejection::Authentication) => {
                                trace!(
                                    "read replica dropped datagram from {peer}: \
                                     missing or invalid MAC"
                                );
                                continue;
                            }
                            Err(InboundRejection::Version(version)) => {
                                trace!(
                                    "read replica dropped datagram from {peer}: wire \
                                     version {version} != {}",
                                    auth::WIRE_VERSION
                                );
                                continue;
                            }
                            Err(InboundRejection::PeerCap { current_len, max }) => {
                                trace!(
                                    "read replica dropped datagram from {peer}: peer cap \
                                     reached ({current_len}/{max})"
                                );
                                continue;
                            }
                            Err(InboundRejection::Replay { seq, stamp }) => {
                                trace!(
                                    "read replica dropped replayed, stale, or replay-capacity \
                                     datagram from {peer}: seq={seq} stamp={stamp}"
                                );
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
                        self.handle_messages(payload, peer, &mut send_buf).await;
                        // Record only a sender that completed a logical protocol payload; an
                        // incomplete fragment transfer is authenticated but has not spoken the
                        // reconciliation protocol yet.
                        self.peers.write().insert(sender, Instant::now());
                    }
                }
            }
        }
    }
}
