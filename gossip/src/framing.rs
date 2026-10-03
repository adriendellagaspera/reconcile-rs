// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Application framing above authenticated datagrams.
//!
//! The outer frame belongs to the network adapter layer: each authenticated datagram contains
//! exactly one complete or fragment frame. Reassembly accepts only replay-checked payloads, so
//! unauthenticated, wrong-version, over-peer-cap, and replayed input cannot allocate state.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use crate::auth::{Payload, Verified};

const COMPLETE_TAG: u8 = 0;
const FRAGMENT_TAG: u8 = 1;

/// Bytes before a complete logical payload inside the authenticated region.
pub const COMPLETE_HEADER_LEN: usize = 1;

/// Bytes before fragment data inside the authenticated region:
/// tag (1) + transfer id (32) + total length (4 LE) + byte offset (4 LE).
pub const FRAGMENT_HEADER_LEN: usize = 41;

/// Runtime bounds for incomplete logical-message reassembly.
#[derive(Clone, Copy, Debug)]
pub struct ReassemblyLimits {
    /// Maximum bytes in one logical protocol message.
    pub max_logical_message_size: usize,
    /// Maximum fragments retained for one logical message.
    pub max_fragments_per_message: usize,
    /// Maximum incomplete logical messages retained for one peer.
    pub max_incomplete_transfers_per_peer: usize,
    /// Maximum fragment bytes retained for one peer.
    pub max_reassembly_bytes_per_peer: usize,
    /// Maximum fragment bytes retained across all peers.
    pub max_total_reassembly_bytes: usize,
    /// Incomplete-transfer inactivity timeout.
    pub reassembly_ttl: Duration,
}

/// A fully authenticated logical payload, either borrowed from a complete datagram or owned after
/// fragment reassembly.
#[derive(Debug)]
pub struct LogicalPayload<'a>(Cow<'a, [u8]>);

impl LogicalPayload<'_> {
    /// Bytes ready for the protocol codec.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Result of accepting one authenticated outer frame.
#[derive(Debug)]
pub enum ReceiveOutcome<'a> {
    /// A complete logical payload is ready for protocol deserialization.
    Complete(LogicalPayload<'a>),
    /// A fragment was retained but the logical payload is still incomplete.
    Pending,
    /// The exact fragment bytes were already retained.
    Duplicate,
}

/// Why an authenticated outer frame could not be accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameError {
    /// The authenticated payload had no framing tag.
    Empty,
    /// The framing tag is unknown to this wire version.
    UnknownTag(u8),
    /// A fragment frame ended before its fixed metadata header.
    TruncatedFragmentHeader,
    /// A fragment carried no bytes.
    EmptyFragment,
    /// Fragment offset/length does not fit within the declared logical length.
    InvalidFragmentBounds,
    /// The declared logical message exceeds the configured maximum.
    LogicalMessageTooLarge,
    /// Retaining another fragment would exceed the per-message fragment-count bound.
    TooManyFragments,
    /// The same transfer id was presented with inconsistent total-length metadata.
    InconsistentMetadata,
    /// Fragment byte ranges overlap without being exact duplicates.
    OverlappingFragment,
    /// A completed transfer did not hash to its content-addressed transfer id.
    HashMismatch,
    /// Reassembly cannot make room without evicting the transfer currently being extended.
    ReassemblyCapacity,
}

impl FrameError {
    /// Whether this rejection was caused by an explicit resource limit.
    pub fn is_limit(self) -> bool {
        matches!(
            self,
            FrameError::LogicalMessageTooLarge
                | FrameError::TooManyFragments
                | FrameError::ReassemblyCapacity
        )
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Empty => write!(f, "empty framing payload"),
            FrameError::UnknownTag(tag) => write!(f, "unknown framing tag {tag}"),
            FrameError::TruncatedFragmentHeader => write!(f, "truncated fragment header"),
            FrameError::EmptyFragment => write!(f, "fragment payload is empty"),
            FrameError::InvalidFragmentBounds => {
                write!(f, "fragment lies outside declared logical length")
            }
            FrameError::LogicalMessageTooLarge => {
                write!(f, "logical message exceeds configured maximum")
            }
            FrameError::TooManyFragments => {
                write!(f, "logical message exceeds fragment-count bound")
            }
            FrameError::InconsistentMetadata => {
                write!(f, "transfer metadata changed for the same id")
            }
            FrameError::OverlappingFragment => write!(f, "fragment overlaps retained bytes"),
            FrameError::HashMismatch => {
                write!(f, "reassembled message does not match transfer id")
            }
            FrameError::ReassemblyCapacity => write!(f, "reassembly byte capacity exhausted"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Side effects of one receive attempt, including deterministic expiry/eviction accounting.
#[derive(Debug)]
pub struct ReceiveReport<'a> {
    /// Accepted payload state, or the rejection.
    pub outcome: Result<ReceiveOutcome<'a>, FrameError>,
    /// Incomplete transfers removed because their TTL elapsed.
    pub expired_transfers: usize,
    /// Incomplete transfers evicted to satisfy peer/global caps.
    pub evicted_transfers: usize,
    /// Fragment bytes retained after this attempt.
    pub retained_bytes: usize,
    /// Whether this datagram was a fragment frame.
    pub fragment_received: bool,
}

#[derive(Debug)]
struct Transfer {
    total_len: usize,
    fragments: BTreeMap<usize, Vec<u8>>,
    retained_bytes: usize,
    last_activity: Instant,
}

#[derive(Default, Debug)]
struct PeerState {
    transfers: HashMap<[u8; 32], Transfer>,
    retained_bytes: usize,
}

/// Bounded, content-addressed incomplete-transfer state.
///
/// The input type makes authentication/version/replay checking a prerequisite for allocation.
#[derive(Debug)]
pub struct Reassembler {
    limits: ReassemblyLimits,
    peers: HashMap<IpAddr, PeerState>,
    total_retained_bytes: usize,
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
        let outcome =
            self.accept_fragment(peer, id, total_len, offset, fragment, now, &mut evicted_transfers);
        self.report(
            outcome,
            expired_transfers,
            evicted_transfers,
            true,
        )
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
        id: [u8; 32],
        total_len: usize,
        offset: usize,
        fragment: &[u8],
        now: Instant,
        evicted: &mut usize,
    ) -> Result<ReceiveOutcome<'a>, FrameError> {
        if fragment.is_empty() {
            return Err(FrameError::EmptyFragment);
        }
        if total_len > self.limits.max_logical_message_size {
            return Err(FrameError::LogicalMessageTooLarge);
        }
        if self.limits.max_fragments_per_message == 0 {
            return Err(FrameError::TooManyFragments);
        }
        let end = offset
            .checked_add(fragment.len())
            .ok_or(FrameError::InvalidFragmentBounds)?;
        if total_len == 0 || offset >= total_len || end > total_len {
            return Err(FrameError::InvalidFragmentBounds);
        }

        if let Some(existing) = self
            .peers
            .get_mut(&peer)
            .and_then(|state| state.transfers.get_mut(&id))
        {
            if existing.total_len != total_len {
                return Err(FrameError::InconsistentMetadata);
            }
            if let Some(bytes) = existing.fragments.get(&offset) {
                if bytes.as_slice() == fragment {
                    existing.last_activity = now;
                    return Ok(ReceiveOutcome::Duplicate);
                }
                return Err(FrameError::OverlappingFragment);
            }
            if overlaps(&existing.fragments, offset, end) {
                return Err(FrameError::OverlappingFragment);
            }
            if existing.fragments.len() >= self.limits.max_fragments_per_message {
                return Err(FrameError::TooManyFragments);
            }
            self.make_byte_room(peer, id, fragment.len(), evicted)?;
        } else {
            self.make_transfer_slot(peer, id, fragment.len(), evicted)?;
            self.make_byte_room(peer, id, fragment.len(), evicted)?;
            self.peers.entry(peer).or_default().transfers.insert(
                id,
                Transfer {
                    total_len,
                    fragments: BTreeMap::new(),
                    retained_bytes: 0,
                    last_activity: now,
                },
            );
        }

        let state = self.peers.get_mut(&peer).expect("transfer peer exists");
        let transfer = state
            .transfers
            .get_mut(&id)
            .expect("transfer exists after capacity checks");
        transfer.fragments.insert(offset, fragment.to_vec());
        transfer.retained_bytes += fragment.len();
        transfer.last_activity = now;
        state.retained_bytes += fragment.len();
        self.total_retained_bytes += fragment.len();

        if !is_complete(transfer) {
            return Ok(ReceiveOutcome::Pending);
        }

        let mut assembled = Vec::with_capacity(transfer.total_len);
        for bytes in transfer.fragments.values() {
            assembled.extend_from_slice(bytes);
        }
        self.remove_transfer(peer, id);
        if *blake3::hash(&assembled).as_bytes() != id {
            return Err(FrameError::HashMismatch);
        }
        Ok(ReceiveOutcome::Complete(LogicalPayload(Cow::Owned(assembled))))
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

    fn oldest_for_peer(
        &self,
        peer: IpAddr,
        exclude: Option<[u8; 32]>,
    ) -> Option<[u8; 32]> {
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

    fn oldest_global(
        &self,
        exclude: Option<(IpAddr, [u8; 32])>,
    ) -> Option<(IpAddr, [u8; 32])> {
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

fn is_complete(transfer: &Transfer) -> bool {
    let mut expected = 0usize;
    for (&offset, bytes) in &transfer.fragments {
        if offset != expected {
            return false;
        }
        expected = expected.saturating_add(bytes.len());
    }
    expected == transfer.total_len
}

/// Maximum complete-frame payload for a total UDP datagram payload budget and auth overhead.
pub fn complete_payload_capacity(
    datagram_payload_budget: usize,
    auth_overhead: usize,
) -> Option<usize> {
    datagram_payload_budget.checked_sub(auth_overhead + COMPLETE_HEADER_LEN)
}

/// Maximum fragment data payload for a total UDP datagram payload budget and auth overhead.
pub fn fragment_payload_capacity(
    datagram_payload_budget: usize,
    auth_overhead: usize,
) -> Option<usize> {
    datagram_payload_budget.checked_sub(auth_overhead + FRAGMENT_HEADER_LEN)
}

/// Write one complete outer frame.
pub fn write_complete(payload: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(COMPLETE_HEADER_LEN + payload.len());
    out.push(COMPLETE_TAG);
    out.extend_from_slice(payload);
}

/// Stable content-addressed id for retransmission of the exact same logical message.
pub fn transfer_id(payload: &[u8]) -> [u8; 32] {
    *blake3::hash(payload).as_bytes()
}

/// Write one fragment outer frame.
pub fn write_fragment(
    id: [u8; 32],
    total_len: usize,
    offset: usize,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), FrameError> {
    let total_len = u32::try_from(total_len).map_err(|_| FrameError::LogicalMessageTooLarge)?;
    let offset = u32::try_from(offset).map_err(|_| FrameError::InvalidFragmentBounds)?;
    out.clear();
    out.reserve(FRAGMENT_HEADER_LEN + payload.len());
    out.push(FRAGMENT_TAG);
    out.extend_from_slice(&id);
    out.extend_from_slice(&total_len.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Authenticator;
    use crate::replay::{ReplayFilter, SenderCounter, FRESHNESS_WINDOW_DEFAULT};

    fn verified<'a>(auth: &Authenticator, wire: &'a [u8]) -> Payload<'a, Verified> {
        auth.open(wire)
            .unwrap()
            .check_version()
            .unwrap()
            .verify_replay(
                &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
                "127.0.0.1".parse().unwrap(),
            )
            .unwrap()
    }

    fn limits() -> ReassemblyLimits {
        ReassemblyLimits {
            max_logical_message_size: 1024,
            max_fragments_per_message: 16,
            max_incomplete_transfers_per_peer: 2,
            max_reassembly_bytes_per_peer: 1024,
            max_total_reassembly_bytes: 2048,
            reassembly_ttl: Duration::from_secs(10),
        }
    }

    #[test]
    fn complete_frame_round_trips_without_reassembly_allocation() {
        let auth = Authenticator::new(None, false).unwrap();
        let counter = SenderCounter::new();
        let mut frame = Vec::new();
        write_complete(b"hello", &mut frame);
        let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
        let mut receiver = Reassembler::new(limits());
        let report = receiver.accept(
            "127.0.0.1".parse().unwrap(),
            verified(&auth, &wire),
            Instant::now(),
        );
        match report.outcome.unwrap() {
            ReceiveOutcome::Complete(payload) => assert_eq!(payload.as_bytes(), b"hello"),
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(receiver.retained_bytes(), 0);
    }

    #[test]
    fn out_of_order_fragments_complete_and_duplicate_reuses_progress() {
        let auth = Authenticator::new(None, false).unwrap();
        let counter = SenderCounter::new();
        let peer = "127.0.0.1".parse().unwrap();
        let message = b"abcdefghij";
        let id = transfer_id(message);
        let now = Instant::now();
        let mut receiver = Reassembler::new(limits());

        fn feed<'a>(
            auth: &Authenticator,
            counter: &SenderCounter,
            peer: IpAddr,
            id: [u8; 32],
            total: usize,
            offset: usize,
            bytes: &[u8],
            now: Instant,
            receiver: &'a mut Reassembler,
        ) -> ReceiveReport<'a> {
            let mut frame = Vec::new();
            write_fragment(id, total, offset, bytes, &mut frame).unwrap();
            let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
            let opened = auth
                .open(&wire)
                .unwrap()
                .check_version()
                .unwrap()
                .verify_replay(
                    &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
                    peer,
                )
                .unwrap();
            receiver.accept(peer, opened, now)
        }

        assert!(matches!(
            feed(&auth, &counter, peer, id, message.len(), 5, b"fghij", now, &mut receiver)
                .outcome
                .unwrap(),
            ReceiveOutcome::Pending
        ));
        assert!(matches!(
            feed(&auth, &counter, peer, id, message.len(), 5, b"fghij", now, &mut receiver)
                .outcome
                .unwrap(),
            ReceiveOutcome::Duplicate
        ));
        let report = feed(
            &auth,
            &counter,
            peer,
            id,
            message.len(),
            0,
            b"abcde",
            now,
            &mut receiver,
        );
        match report.outcome.unwrap() {
            ReceiveOutcome::Complete(payload) => assert_eq!(payload.as_bytes(), message),
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(receiver.retained_bytes(), 0);
    }

    #[test]
    fn ttl_and_peer_transfer_cap_evict_deterministically() {
        let peer = "127.0.0.1".parse().unwrap();
        let mut receiver = Reassembler::new(ReassemblyLimits {
            max_incomplete_transfers_per_peer: 1,
            reassembly_ttl: Duration::from_millis(10),
            ..limits()
        });
        let auth = Authenticator::new(None, false).unwrap();
        let counter = SenderCounter::new();
        let start = Instant::now();

        for (index, message) in [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .enumerate()
        {
            let id = transfer_id(message);
            let mut frame = Vec::new();
            write_fragment(id, message.len(), 0, &message[..1], &mut frame).unwrap();
            let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
            let report = receiver.accept(
                peer,
                verified(&auth, &wire),
                start + Duration::from_millis(index as u64),
            );
            assert_eq!(report.evicted_transfers, usize::from(index == 1));
        }

        assert_eq!(receiver.expire(start + Duration::from_millis(20)), 1);
        assert_eq!(receiver.retained_bytes(), 0);
    }

    #[test]
    fn malformed_and_over_limit_fragment_metadata_is_rejected() {
        let auth = Authenticator::new(None, false).unwrap();
        let counter = SenderCounter::new();
        let peer = "127.0.0.1".parse().unwrap();
        let mut receiver = Reassembler::new(limits());

        let truncated = vec![FRAGMENT_TAG; FRAGMENT_HEADER_LEN - 1];
        let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &truncated);
        assert_eq!(
            receiver
                .accept(peer, verified(&auth, &wire), Instant::now())
                .outcome
                .unwrap_err(),
            FrameError::TruncatedFragmentHeader
        );

        let mut frame = Vec::new();
        let id = transfer_id(&vec![0u8; 2048]);
        write_fragment(id, 2048, 0, b"x", &mut frame).unwrap();
        let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
        assert_eq!(
            receiver
                .accept(peer, verified(&auth, &wire), Instant::now())
                .outcome
                .unwrap_err(),
            FrameError::LogicalMessageTooLarge
        );
        assert_eq!(receiver.retained_bytes(), 0);
    }
}
