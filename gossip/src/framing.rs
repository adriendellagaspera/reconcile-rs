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
use web_time::{Duration, Instant};

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

    /// Detach this logical payload from the receive buffer.
    pub fn into_owned(self) -> LogicalPayload<'static> {
        LogicalPayload(Cow::Owned(self.0.into_owned()))
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
    // Rate limit missing reports without changing the inactivity/eviction TTL.
    last_missing_request: Option<Instant>,
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

mod reassembly;
mod recovery;

pub use recovery::{
    fragment_metadata, max_missing_ranges_for_budget, parse_recovery_control, write_completion_ack,
    write_missing_report, FragmentMetadata, MissingRequest, RecoveryControl, RecoveryControlError,
    COMPLETION_ACK_LEN, COMPLETION_ACK_TAG, MISSING_RANGE_LEN, MISSING_REPORT_HEADER_LEN,
    MISSING_REPORT_TAG,
};

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
mod tests;
