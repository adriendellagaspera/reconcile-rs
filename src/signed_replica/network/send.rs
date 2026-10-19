// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

/// Cap the total outbound work in a single caller-driven reconciliation tick. A large
/// transfer may consume the whole tick; round-robin peer selection prevents starvation.
const MAX_FRAMES_PER_TICK: usize = 8_192;

impl<K: Key + Hash, V: Value, A: Clone + Eq> SignedReplica<K, V, A> {
    /// Send a cryptographically signed hello to a *provisioned* substrate endpoint.
    /// This does not treat the supplied address as a peer identity or confirmed route.
    /// The recipient must return a nonce challenge before either side dispatches data.
    ///
    /// # Errors
    /// When the signed hello cannot fit the selected substrate MTU.
    pub fn probe(&self, endpoint: A) -> io::Result<RoutedDatagram<A>> {
        let message: SignedMessage<K, V> = SignedMessage::Hello;
        let mut frame = Vec::new();
        write_complete(&serialize(&message)?, &mut frame);
        let bytes = self.boundary.seal(&frame).map_err(|err| invalid(format!("{err:?}")))?;
        Ok(RoutedDatagram { destination: endpoint, bytes })
    }

    /// Progress anti-entropy on confirmed signed routes and request missing fragment ranges.
    ///
    /// Use a caller-chosen periodic cadence and dispatch the returned datagrams through the
    /// physical substrate. Each tick sends at most one dated entry per confirmed peer, rotated
    /// fairly across the peer list, and caps aggregate datagram work.
    ///
    /// # Errors
    /// A retained entry exceeds configured message/fragment limits or signing fails.
    pub fn reconcile(&mut self, now: Instant) -> io::Result<Vec<RoutedDatagram<A>>> {
        self.reassembler.expire(now);
        self.recovery.expire(now, self.framing);
        let mut outgoing = self.retry_missing(now)?;
        if self.read_only {
            return Ok(outgoing);
        }

        let mut peers = self.routes.confirmed_peers();
        if peers.is_empty() || self.entries.is_empty() {
            return Ok(outgoing);
        }
        let peer_count = peers.len();
        peers.rotate_left(self.next_peer_index % peer_count);
        let mut visited = 0;
        for peer in peers {
            if !self.authorities.contains(&peer.peer_id()) && !self.readers.contains(&peer.peer_id()) {
                visited += 1;
                continue;
            }
            let cursor = self.outbound_cursors.get(&peer.peer_id()).cloned().flatten();
            let next = self.entries.iter().find(|(k, _)| cursor.as_ref().is_none_or(|last| *k > last));
            // Wrap to the oldest key for continuous anti-entropy, including lost ACK recovery.
            let next = next.or_else(|| self.entries.iter().next());
            let Some((key, entry)) = next else {
                break;
            };
            let (key, entry) = (key.clone(), entry.clone());
            let encoded = serialize(&SignedMessage::Update(key.clone(), entry))?;
            let frames = self.required_fragments(encoded.len())?;
            if outgoing.len().saturating_add(frames) > MAX_FRAMES_PER_TICK {
                if visited == 0 && outgoing.is_empty() {
                    return Err(invalid("one encoded signed record exceeds the per-tick datagram cap"));
                }
                break;
            }
            outgoing.extend(self.send_encoded(peer, &encoded, now)?);
            self.outbound_cursors.insert(peer.peer_id(), Some(key));
            visited += 1;
        }
        self.next_peer_index = self.next_peer_index.wrapping_add(visited);
        Ok(outgoing)
    }

    pub(super) fn ensure_entry_fits(
        &self,
        key: &K,
        entry: &Entry<Timestamp, V>,
    ) -> io::Result<()> {
        let bytes = serialize(&SignedMessage::Update(key.clone(), entry.clone()))?;
        self.required_fragments(bytes.len())?;
        Ok(())
    }

    fn required_fragments(&self, logical_bytes: usize) -> io::Result<usize> {
        if logical_bytes > self.framing.max_logical_message_size {
            return Err(invalid("signed logical record exceeds configured size cap"));
        }
        let budget = self.boundary.payload_budget();
        if logical_bytes.saturating_add(gossip::framing::COMPLETE_HEADER_LEN) <= budget {
            return Ok(1);
        }
        let chunk = budget.checked_sub(FRAGMENT_HEADER_LEN)
            .filter(|n| *n > 0)
            .ok_or_else(|| invalid("signed fragment headers exceed transport budget"))?;
        let count = logical_bytes.div_ceil(chunk);
        if count > self.framing.max_fragments_per_message || count > MAX_FRAMES_PER_TICK {
            return Err(invalid("signed record exceeds bounded fragment count"));
        }
        Ok(count)
    }

    pub(super) fn send_message(
        &mut self,
        peer: VerifiedPeerId,
        message: &SignedMessage<K, V>,
        now: Instant,
    ) -> io::Result<Vec<RoutedDatagram<A>>> {
        let bytes = serialize(message)?;
        self.send_encoded(peer, &bytes, now)
    }

    fn send_encoded(
        &mut self,
        peer: VerifiedPeerId,
        bytes: &[u8],
        now: Instant,
    ) -> io::Result<Vec<RoutedDatagram<A>>> {
        let count = self.required_fragments(bytes.len())?;
        let mut frames = Vec::with_capacity(count);
        let mut frame = Vec::new();
        if count == 1 {
            write_complete(bytes, &mut frame);
            frames.push(self.routed_frame(peer, &frame)?);
            return Ok(frames);
        }

        let chunk = self.boundary.payload_budget() - FRAGMENT_HEADER_LEN;
        let transfer = transfer_id(bytes);
        if self.framing.selective_recovery {
            self.recovery.retain(peer, transfer, bytes, now, self.framing);
        }
        for (index, slice) in bytes.chunks(chunk).enumerate() {
            write_fragment(transfer, bytes.len(), index * chunk, slice, &mut frame)
                .map_err(|err| invalid(err.to_string()))?;
            frames.push(self.routed_frame(peer, &frame)?);
        }
        Ok(frames)
    }

    pub(super) fn routed_frame(
        &self,
        peer: VerifiedPeerId,
        frame: &[u8],
    ) -> io::Result<RoutedDatagram<A>> {
        self.routes.seal_to(&self.boundary, peer.peer_id(), frame)
            .map_err(|err| invalid(format!("{err:?}")))
    }

    fn retry_missing(&mut self, now: Instant) -> io::Result<Vec<RoutedDatagram<A>>> {
        if !self.framing.selective_recovery {
            return Ok(Vec::new());
        }
        let max_ranges = max_missing_ranges_for_budget(
            self.boundary.payload_budget(),
            0,
            self.framing.max_missing_ranges_per_report,
        );
        let requests = self.reassembler.poll_idle_missing(
            now,
            Duration::from_secs(2),
            Duration::from_secs(3),
            max_ranges,
            16,
        );
        let mut out = Vec::new();
        for request in requests {
            if self.routes.route(request.peer.peer_id()).is_none() {
                continue;
            }
            let mut frame = Vec::new();
            write_missing_report(request.transfer_id, &request.ranges, &mut frame)
                .map_err(|err| invalid(err.to_string()))?;
            out.push(self.routed_frame(request.peer, &frame)?);
        }
        Ok(out)
    }

    pub(super) fn resend_missing(
        &mut self,
        peer: VerifiedPeerId,
        id: [u8; 32],
        ranges: &[(u32, u32)],
        now: Instant,
    ) -> io::Result<Vec<RoutedDatagram<A>>> {
        let Some(transfer) = self.recovery.payload_for_report(peer, id, now, self.framing) else {
            return Ok(Vec::new());
        };
        let bytes = transfer.payload;
        let chunk = self.boundary.payload_budget() - FRAGMENT_HEADER_LEN;
        let mut out = Vec::new();
        let mut frame = Vec::new();
        for &(offset, len) in ranges {
            let start = offset as usize;
            let end = start.saturating_add(len as usize).min(bytes.len());
            if start >= end {
                continue;
            }
            for (index, slice) in bytes[start..end].chunks(chunk).enumerate() {
                write_fragment(id, bytes.len(), start + index * chunk, slice, &mut frame)
                    .map_err(|err| invalid(err.to_string()))?;
                out.push(self.routed_frame(peer, &frame)?);
                if out.len() >= MAX_FRAMES_PER_TICK {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
}
