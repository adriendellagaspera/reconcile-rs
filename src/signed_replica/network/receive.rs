// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

/// The signed ingress path is deliberately *separate* from legacy `Replica::run`.
/// Neither cluster-wide PSK proofs nor a source IP can grant signed causal membership.
impl<K: Key + Hash, V: Value, A: Clone + Eq> SignedReplica<K, V, A> {
    /// Consume a datagram from an untrusted physical source address, yielding any signed
    /// challenge, challenge response, selective-recovery fragment or tombstone ACK.
    ///
    /// Signature, allowlist, inner MAC/version and principal replay are always checked before
    /// returnable-route admission and protocol deserialization. Invalid packets are dropped.
    /// Signed protocol messages from unconfirmed routes never enter causal state.
    ///
    /// # Errors
    /// If constructing an authorized outbound response exceeds the configured framing cap.
    pub fn receive(
        &mut self,
        source: A,
        datagram: &[u8],
        now: Instant,
    ) -> io::Result<Vec<RoutedDatagram<A>>> {
        let allowed_authorities = &self.authorities;
        let allowed_readers = &self.readers;
        let current_members = self.members.len();
        let max = self.max_peer_cap;
        let result = self.routes.receive(
            &self.boundary,
            datagram,
            source,
            |peer| {
                if allowed_authorities.contains(&peer) || allowed_readers.contains(&peer) {
                    (true, current_members)
                } else {
                    // In particular, a decommissioned but still cryptographically valid
                    // signer cannot regain access before the refreshed allowlist is installed.
                    (false, max)
                }
            },
            &mut self.reassembler,
            now,
        );
        let inbound = match result {
            Ok(inbound) => inbound,
            Err(_) => return Ok(Vec::new()),
        };
        match inbound {
            SignedRouteReceive::Ignored => Ok(Vec::new()),
            SignedRouteReceive::Probing { challenge, .. } => {
                Ok(challenge.into_iter().collect())
            }
            SignedRouteReceive::Reply(response) => Ok(vec![response]),
            SignedRouteReceive::Confirmed(peer) => {
                self.recovery.record_capability(peer, now, self.framing, max);
                Ok(Vec::new())
            }
            SignedRouteReceive::Recovery { peer, control } => {
                if !self.authorities.contains(&peer.peer_id()) && !self.readers.contains(&peer.peer_id()) {
                    return Ok(Vec::new());
                }
                match control {
                    RecoveryControl::Missing { transfer_id, ranges } => {
                        self.resend_missing(peer, transfer_id, &ranges, now)
                    }
                    RecoveryControl::CompleteAck { transfer_id } => {
                        self.recovery.complete(peer, transfer_id);
                        Ok(Vec::new())
                    }
                }
            }
            SignedRouteReceive::Framed { peer, report } => {
                let was_fragment = report.fragment_received;
                let payload = match report.outcome {
                    Ok(ReceiveOutcome::Complete(payload)) => payload,
                    _ => return Ok(Vec::new()),
                };
                let bytes = payload.as_bytes();
                // Each frame carries exactly one tagged typed message. Two complete messages,
                // or a partial/truncated trailing message, must not result in partial effects.
                let decoded = match gossip::bincode::decode_stream::<SignedMessage<K, V>>(bytes, 2) {
                    Ok(decoded) if decoded.len() == 1 => decoded,
                    _ => return Ok(Vec::new()),
                };
                let mut responses = Vec::new();
                if was_fragment {
                    let mut frame = Vec::new();
                    write_completion_ack(transfer_id(bytes), &mut frame);
                    responses.push(self.routed_frame(peer, &frame)?);
                }
                let message = decoded.into_iter().next().expect("exactly one parsed message");
                if let Some(reply) = self.handle_verified_message(peer, message, now)? {
                    responses.extend(reply);
                }
                Ok(responses)
            }
        }
    }

    fn handle_verified_message(
        &mut self,
        peer: VerifiedPeerId,
        message: SignedMessage<K, V>,
        now: Instant,
    ) -> io::Result<Option<Vec<RoutedDatagram<A>>>> {
        match message {
            SignedMessage::Hello => Ok(None),
            SignedMessage::Update(key, entry) => {
                if !self.authorities.contains(&peer.peer_id()) {
                    return Ok(None);
                }
                // Read-only replicas may consume authoritative data but never emit dated ACKs.
                if !self.read_only {
                    self.admit_authority(peer);
                }
                let ack_version = self.apply_remote_entry(peer, key.clone(), entry);
                if self.read_only {
                    return Ok(None);
                }
                match ack_version {
                    Some(version) => Ok(Some(self.send_message(
                        peer,
                        &SignedMessage::Ack(key, version),
                        now,
                    )?)),
                    None => Ok(None),
                }
            }
            SignedMessage::Ack(key, version) => {
                if !self.read_only && self.authorities.contains(&peer.peer_id()) {
                    self.admit_authority(peer);
                    self.accept_ack(peer, key, version);
                }
                Ok(None)
            }
        }
    }
}
