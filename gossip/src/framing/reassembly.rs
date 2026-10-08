// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::auth::{Payload, Verified};

use super::*;

struct FragmentInput<'a> {
    id: [u8; 32],
    total_len: usize,
    offset: usize,
    bytes: &'a [u8],
}

impl Reassembler {
    /// Create an empty receiver with explicit memory/state limits.
    pub fn new(limits: ReassemblyLimits) -> Self {
        Reassembler {
            limits,
            peers: HashMap::new(),
            total_retained_bytes: 0,
        }
    }

    /// Bytes currently retained across all incomplete transfers.
    pub fn retained_bytes(&self) -> usize {
        self.total_retained_bytes
    }

    /// Remove transfers whose inactivity TTL elapsed, returning the number removed.
    pub fn expire(&mut self, now: Instant) -> usize {
        let ttl = self.limits.reassembly_ttl;
        let mut expired = Vec::new();
        for (&peer, state) in &self.peers {
            for (&id, transfer) in &state.transfers {
                if now.saturating_duration_since(transfer.last_activity) >= ttl {
                    expired.push((peer, id));
                }
            }
        }
        let count = expired.len();
        for (peer, id) in expired {
            self.remove_transfer(peer, id);
        }
        count
    }

    /// Accept one authenticated/replay-checked outer frame from a peer.
    pub fn accept<'a>(
        &mut self,
        peer: IpAddr,
        payload: Payload<'a, Verified>,
        now: Instant,
    ) -> ReceiveReport<'a> {
        let expired_transfers = self.expire(now);
        let bytes = payload.into_bytes();
        let Some(&tag) = bytes.first() else {
            return self.report(Err(FrameError::Empty), expired_transfers, 0, false);
        };

        if tag == COMPLETE_TAG {
            let logical_len = bytes.len().saturating_sub(COMPLETE_HEADER_LEN);
            if logical_len > self.limits.max_logical_message_size {
                return self.report(
                    Err(FrameError::LogicalMessageTooLarge),
                    expired_transfers,
                    0,
                    false,
                );
            }
            let logical = match bytes {
                Cow::Borrowed(raw) => Cow::Borrowed(&raw[COMPLETE_HEADER_LEN..]),
                Cow::Owned(mut raw) => {
                    raw.drain(..COMPLETE_HEADER_LEN);
                    Cow::Owned(raw)
                }
            };
            return self.report(
                Ok(ReceiveOutcome::Complete(LogicalPayload(logical))),
                expired_transfers,
                0,
                false,
            );
        }

        if tag != FRAGMENT_TAG {
            return self.report(
                Err(FrameError::UnknownTag(tag)),
                expired_transfers,
                0,
                false,
            );
        }

        if bytes.len() < FRAGMENT_HEADER_LEN {
            return self.report(
                Err(FrameError::TruncatedFragmentHeader),
                expired_transfers,
                0,
                true,
            );
        }

        let mut id = [0u8; 32];
        id.copy_from_slice(&bytes[1..33]);
        let total_len = u32::from_le_bytes(bytes[33..37].try_into().unwrap()) as usize;
        let offset = u32::from_le_bytes(bytes[37..41].try_into().unwrap()) as usize;
        let fragment = &bytes[FRAGMENT_HEADER_LEN..];

        let mut evicted_transfers = 0;
        let outcome = self.accept_fragment(
            peer,
            FragmentInput {
                id,
                total_len,
                offset,
                bytes: fragment,
            },
            now,
            &mut evicted_transfers,
        );
        self.report(outcome, expired_transfers, evicted_transfers, true)
    }

    fn report<'a>(
        &self,
        outcome: Result<ReceiveOutcome<'a>, FrameError>,
        expired_transfers: usize,
        evicted_transfers: usize,
        fragment_received: bool,
    ) -> ReceiveReport<'a> {
        ReceiveReport {
            outcome,
            expired_transfers,
            evicted_transfers,
            retained_bytes: self.total_retained_bytes,
            fragment_received,
        }
    }

    fn accept_fragment<'a>(
        &mut self,
        peer: IpAddr,
        fragment: FragmentInput<'_>,
        now: Instant,
        evicted: &mut usize,
    ) -> Result<ReceiveOutcome<'a>, FrameError> {
        if fragment.bytes.is_empty() {
            return Err(FrameError::EmptyFragment);
        }
        if fragment.total_len > self.limits.max_logical_message_size {
            return Err(FrameError::LogicalMessageTooLarge);
        }
        if self.limits.max_fragments_per_message == 0 {
            return Err(FrameError::TooManyFragments);
        }
        let end = fragment
            .offset
            .checked_add(fragment.bytes.len())
            .ok_or(FrameError::InvalidFragmentBounds)?;
        // Fragments are non-empty, so an offset at or beyond total_len necessarily makes
        // end exceed total_len as well. The end check therefore covers zero-length logical
        // messages, offsets at/past the end, and fragments crossing the declared end.
        if end > fragment.total_len {
            return Err(FrameError::InvalidFragmentBounds);
        }

        if let Some(existing) = self
            .peers
            .get_mut(&peer)
            .and_then(|state| state.transfers.get_mut(&fragment.id))
        {
            if existing.total_len != fragment.total_len {
                return Err(FrameError::InconsistentMetadata);
            }
            if let Some(bytes) = existing.fragments.get(&fragment.offset) {
                if bytes.as_slice() == fragment.bytes {
                    existing.last_activity = now;
                    return Ok(ReceiveOutcome::Duplicate);
                }
                return Err(FrameError::OverlappingFragment);
            }
            if overlaps(&existing.fragments, fragment.offset, end) {
                return Err(FrameError::OverlappingFragment);
            }
            if existing.fragments.len() >= self.limits.max_fragments_per_message {
                return Err(FrameError::TooManyFragments);
            }
            self.make_byte_room(peer, fragment.id, fragment.bytes.len(), evicted)?;
        } else {
            self.make_transfer_slot(peer, fragment.id, fragment.bytes.len(), evicted)?;
            self.make_byte_room(peer, fragment.id, fragment.bytes.len(), evicted)?;
            self.peers.entry(peer).or_default().transfers.insert(
                fragment.id,
                Transfer {
                    total_len: fragment.total_len,
                    fragments: BTreeMap::new(),
                    retained_bytes: 0,
                    last_activity: now,
                    last_missing_request: None,
                },
            );
        }

        let state = self.peers.get_mut(&peer).expect("transfer peer exists");
        let transfer = state
            .transfers
            .get_mut(&fragment.id)
            .expect("transfer exists after capacity checks");
        transfer
            .fragments
            .insert(fragment.offset, fragment.bytes.to_vec());
        transfer.retained_bytes += fragment.bytes.len();
        transfer.last_activity = now;
        state.retained_bytes += fragment.bytes.len();
        self.total_retained_bytes += fragment.bytes.len();

        // Every accepted fragment is in-bounds and non-overlapping. Their retained byte sum
        // therefore reaches total_len iff their union covers the whole logical message; no
        // O(fragment_count) rescan is needed after every insertion.
        if transfer.retained_bytes != transfer.total_len {
            return Ok(ReceiveOutcome::Pending);
        }

        let mut assembled = Vec::with_capacity(transfer.total_len);
        for bytes in transfer.fragments.values() {
            assembled.extend_from_slice(bytes);
        }
        self.remove_transfer(peer, fragment.id);
        if *blake3::hash(&assembled).as_bytes() != fragment.id {
            return Err(FrameError::HashMismatch);
        }
        Ok(ReceiveOutcome::Complete(LogicalPayload(Cow::Owned(
            assembled,
        ))))
    }

    fn make_transfer_slot(
        &mut self,
        peer: IpAddr,
        protected_id: [u8; 32],
        incoming_bytes: usize,
        evicted: &mut usize,
    ) -> Result<(), FrameError> {
        if incoming_bytes > self.limits.max_reassembly_bytes_per_peer
            || incoming_bytes > self.limits.max_total_reassembly_bytes
        {
            return Err(FrameError::ReassemblyCapacity);
        }
        while self
            .peers
            .get(&peer)
            .map_or(0, |state| state.transfers.len())
            >= self.limits.max_incomplete_transfers_per_peer
        {
            let Some(id) = self.oldest_for_peer(peer, Some(protected_id)) else {
                return Err(FrameError::ReassemblyCapacity);
            };
            self.remove_transfer(peer, id);
            *evicted += 1;
        }
        Ok(())
    }

    fn make_byte_room(
        &mut self,
        peer: IpAddr,
        protected_id: [u8; 32],
        incoming_bytes: usize,
        evicted: &mut usize,
    ) -> Result<(), FrameError> {
        while self
            .peers
            .get(&peer)
            .map_or(0, |state| state.retained_bytes)
            .saturating_add(incoming_bytes)
            > self.limits.max_reassembly_bytes_per_peer
        {
            let Some(id) = self.oldest_for_peer(peer, Some(protected_id)) else {
                return Err(FrameError::ReassemblyCapacity);
            };
            self.remove_transfer(peer, id);
            *evicted += 1;
        }
        while self.total_retained_bytes.saturating_add(incoming_bytes)
            > self.limits.max_total_reassembly_bytes
        {
            let Some((old_peer, old_id)) = self.oldest_global(Some((peer, protected_id))) else {
                return Err(FrameError::ReassemblyCapacity);
            };
            self.remove_transfer(old_peer, old_id);
            *evicted += 1;
        }
        Ok(())
    }

    fn oldest_for_peer(&self, peer: IpAddr, exclude: Option<[u8; 32]>) -> Option<[u8; 32]> {
        self.peers
            .get(&peer)?
            .transfers
            .iter()
            .filter_map(|(&id, transfer)| {
                (Some(id) != exclude).then_some((transfer.last_activity, id))
            })
            .min()
            .map(|(_, id)| id)
    }

    fn oldest_global(&self, exclude: Option<(IpAddr, [u8; 32])>) -> Option<(IpAddr, [u8; 32])> {
        self.peers
            .iter()
            .flat_map(|(&peer, state)| {
                state.transfers.iter().filter_map(move |(&id, transfer)| {
                    (Some((peer, id)) != exclude).then_some((transfer.last_activity, peer, id))
                })
            })
            .min()
            .map(|(_, peer, id)| (peer, id))
    }

    fn remove_transfer(&mut self, peer: IpAddr, id: [u8; 32]) {
        let mut remove_peer = false;
        if let Some(state) = self.peers.get_mut(&peer) {
            if let Some(transfer) = state.transfers.remove(&id) {
                state.retained_bytes = state.retained_bytes.saturating_sub(transfer.retained_bytes);
                self.total_retained_bytes = self
                    .total_retained_bytes
                    .saturating_sub(transfer.retained_bytes);
            }
            remove_peer = state.transfers.is_empty();
        }
        if remove_peer {
            self.peers.remove(&peer);
        }
    }
}

fn overlaps(fragments: &BTreeMap<usize, Vec<u8>>, start: usize, end: usize) -> bool {
    if let Some((&offset, bytes)) = fragments.range(..start).next_back() {
        if offset.saturating_add(bytes.len()) > start {
            return true;
        }
    }
    fragments
        .range(start..)
        .next()
        .is_some_and(|(&offset, _)| offset < end)
}
