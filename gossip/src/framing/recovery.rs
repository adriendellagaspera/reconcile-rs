// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Selective-recovery control framing layered beside complete/fragment data frames.

use std::net::IpAddr;

use super::{Reassembler, FRAGMENT_HEADER_LEN, FRAGMENT_TAG};

/// Outer-frame tag for a compact missing-byte-range report.
pub const MISSING_REPORT_TAG: u8 = 2;
/// Outer-frame tag acknowledging complete reassembly of a fragmented transfer.
pub const COMPLETION_ACK_TAG: u8 = 3;
/// Fixed bytes before missing ranges: tag + transfer id + u16 range count.
pub const MISSING_REPORT_HEADER_LEN: usize = 35;
/// Bytes in one missing range: u32 offset + u32 length.
pub const MISSING_RANGE_LEN: usize = 8;
/// Complete-ack frame size: tag + transfer id.
pub const COMPLETION_ACK_LEN: usize = 33;

/// Metadata readable from one structurally complete fragment header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FragmentMetadata {
    /// Content-derived transfer identifier.
    pub transfer_id: [u8; 32],
    /// Total logical payload bytes.
    pub total_len: usize,
    /// Byte offset of this fragment.
    pub offset: usize,
    /// Bytes carried by this fragment.
    pub payload_len: usize,
}

impl FragmentMetadata {
    /// Whether this fragment reaches the logical payload's declared end.
    pub fn is_terminal(self) -> bool {
        self.offset
            .checked_add(self.payload_len)
            .is_some_and(|end| end == self.total_len)
    }
}

/// Inspect a fragment header without mutating reassembly state.
///
/// Returns None for non-fragment frames and for a structurally truncated fragment; the normal
/// reassembler remains authoritative for validation and error reporting.
pub fn fragment_metadata(bytes: &[u8]) -> Option<FragmentMetadata> {
    if bytes.first().copied() != Some(FRAGMENT_TAG) || bytes.len() <= FRAGMENT_HEADER_LEN {
        return None;
    }
    let mut transfer_id = [0u8; 32];
    transfer_id.copy_from_slice(&bytes[1..33]);
    let total_len = u32::from_le_bytes(bytes[33..37].try_into().ok()?) as usize;
    let offset = u32::from_le_bytes(bytes[37..41].try_into().ok()?) as usize;
    Some(FragmentMetadata {
        transfer_id,
        total_len,
        offset,
        payload_len: bytes.len() - FRAGMENT_HEADER_LEN,
    })
}

/// Authenticated selective-recovery control message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryControl {
    /// Request retransmission of non-overlapping byte ranges for one content-addressed transfer.
    Missing {
        /// Content-addressed transfer identifier.
        transfer_id: [u8; 32],
        /// Sorted, non-overlapping `(offset, length)` ranges.
        ranges: Vec<(u32, u32)>,
    },
    /// Acknowledge that the transfer reassembled completely.
    CompleteAck {
        /// Content-addressed transfer identifier.
        transfer_id: [u8; 32],
    },
}

/// Why an authenticated recovery-control frame is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryControlError {
    /// The fixed control header is truncated.
    Truncated,
    /// The encoded range count does not match the frame length.
    InvalidLength,
    /// The range count exceeds the configured control-state bound.
    TooManyRanges,
    /// A range is empty, overflows, or overlaps/precedes the preceding range.
    InvalidRange,
}

impl std::fmt::Display for RecoveryControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated recovery-control frame"),
            Self::InvalidLength => write!(f, "invalid recovery-control length"),
            Self::TooManyRanges => write!(f, "too many missing ranges"),
            Self::InvalidRange => write!(f, "invalid missing range"),
        }
    }
}

impl std::error::Error for RecoveryControlError {}

/// Maximum number of missing ranges that fit one authenticated datagram budget.
pub fn max_missing_ranges_for_budget(
    datagram_payload_budget: usize,
    auth_overhead: usize,
    configured_max: usize,
) -> usize {
    datagram_payload_budget
        .checked_sub(auth_overhead + MISSING_REPORT_HEADER_LEN)
        .map_or(0, |bytes| bytes / MISSING_RANGE_LEN)
        .min(configured_max)
        .min(u16::MAX as usize)
}

/// Encode one missing-range report.
pub fn write_missing_report(
    transfer_id: [u8; 32],
    ranges: &[(u32, u32)],
    out: &mut Vec<u8>,
) -> Result<(), RecoveryControlError> {
    let count = u16::try_from(ranges.len()).map_err(|_| RecoveryControlError::TooManyRanges)?;
    validate_ranges(ranges)?;

    out.clear();
    out.reserve(MISSING_REPORT_HEADER_LEN + ranges.len() * MISSING_RANGE_LEN);
    out.push(MISSING_REPORT_TAG);
    out.extend_from_slice(&transfer_id);
    out.extend_from_slice(&count.to_le_bytes());
    for &(offset, len) in ranges {
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    Ok(())
}

/// Encode one completion acknowledgement.
pub fn write_completion_ack(transfer_id: [u8; 32], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(COMPLETION_ACK_LEN);
    out.push(COMPLETION_ACK_TAG);
    out.extend_from_slice(&transfer_id);
}

/// Parse a recovery-control frame. Returns `Ok(None)` for ordinary framing tags.
pub fn parse_recovery_control(
    bytes: &[u8],
    max_missing_ranges: usize,
) -> Result<Option<RecoveryControl>, RecoveryControlError> {
    let Some(&tag) = bytes.first() else {
        return Ok(None);
    };
    match tag {
        MISSING_REPORT_TAG => {
            if bytes.len() < MISSING_REPORT_HEADER_LEN {
                return Err(RecoveryControlError::Truncated);
            }
            let mut transfer_id = [0u8; 32];
            transfer_id.copy_from_slice(&bytes[1..33]);
            let count = u16::from_le_bytes([bytes[33], bytes[34]]) as usize;
            if count > max_missing_ranges {
                return Err(RecoveryControlError::TooManyRanges);
            }
            let expected = MISSING_REPORT_HEADER_LEN
                .checked_add(count.saturating_mul(MISSING_RANGE_LEN))
                .ok_or(RecoveryControlError::InvalidLength)?;
            if bytes.len() != expected {
                return Err(RecoveryControlError::InvalidLength);
            }
            let mut ranges = Vec::with_capacity(count);
            for chunk in bytes[MISSING_REPORT_HEADER_LEN..].chunks_exact(MISSING_RANGE_LEN) {
                let offset = u32::from_le_bytes(chunk[..4].try_into().unwrap());
                let len = u32::from_le_bytes(chunk[4..].try_into().unwrap());
                ranges.push((offset, len));
            }
            validate_ranges(&ranges)?;
            Ok(Some(RecoveryControl::Missing {
                transfer_id,
                ranges,
            }))
        }
        COMPLETION_ACK_TAG => {
            if bytes.len() < COMPLETION_ACK_LEN {
                return Err(RecoveryControlError::Truncated);
            }
            if bytes.len() != COMPLETION_ACK_LEN {
                return Err(RecoveryControlError::InvalidLength);
            }
            let mut transfer_id = [0u8; 32];
            transfer_id.copy_from_slice(&bytes[1..]);
            Ok(Some(RecoveryControl::CompleteAck { transfer_id }))
        }
        _ => Ok(None),
    }
}

fn validate_ranges(ranges: &[(u32, u32)]) -> Result<(), RecoveryControlError> {
    let mut previous_end = 0u32;
    for &(offset, len) in ranges {
        if len == 0 {
            return Err(RecoveryControlError::InvalidRange);
        }
        let end = offset
            .checked_add(len)
            .ok_or(RecoveryControlError::InvalidRange)?;
        if offset < previous_end {
            return Err(RecoveryControlError::InvalidRange);
        }
        previous_end = end;
    }
    Ok(())
}

/// One bounded request for the missing bytes of an authenticated incomplete transfer.
pub struct MissingRequest {
    /// IP identity of the peer that supplied the retained authenticated fragments.
    pub peer: IpAddr,
    /// Stable content-derived transfer ID.
    pub transfer_id: [u8; 32],
    /// Bounded, sorted missing byte ranges.
    pub ranges: Vec<(u32, u32)>,
}

fn missing_ranges_for_transfer(transfer: &super::Transfer, max_ranges: usize) -> Vec<(u32, u32)> {
    if max_ranges == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut cursor = 0usize;
    for (&offset, bytes) in &transfer.fragments {
        if cursor < offset {
            ranges.push((cursor as u32, (offset - cursor) as u32));
            if ranges.len() == max_ranges {
                return ranges;
            }
        }
        cursor = offset + bytes.len();
    }
    // An inner gap already returns at the exact limit; the remaining budget is positive.
    if cursor < transfer.total_len {
        ranges.push((cursor as u32, (transfer.total_len - cursor) as u32));
    }
    ranges
}

impl Reassembler {
    /// Return up to `max_ranges` currently missing byte ranges for one incomplete transfer.
    ///
    /// The ranges are derived from already-authenticated retained fragments; no additional
    /// receiver-side recovery state is allocated.
    pub fn missing_ranges(
        &self,
        peer: IpAddr,
        transfer_id: [u8; 32],
        max_ranges: usize,
    ) -> Vec<(u32, u32)> {
        if max_ranges == 0 {
            return Vec::new();
        }
        let Some(transfer) = self
            .peers
            .get(&peer)
            .and_then(|state| state.transfers.get(&transfer_id))
        else {
            return Vec::new();
        };

        missing_ranges_for_transfer(transfer, max_ranges)
    }
    /// Emit a report at most once per interval even when fresh authenticated duplicates of
    /// the terminal fragment repeatedly arrive. Shares the timer with periodic idle retries.
    pub fn missing_ranges_if_due(
        &mut self,
        peer: IpAddr,
        transfer_id: [u8; 32],
        now: std::time::Instant,
        retry_interval: std::time::Duration,
        max_ranges: usize,
    ) -> Vec<(u32, u32)> {
        if max_ranges == 0 {
            return Vec::new();
        }
        let Some(transfer) = self
            .peers
            .get_mut(&peer)
            .and_then(|state| state.transfers.get_mut(&transfer_id))
        else {
            return Vec::new();
        };
        if transfer
            .last_missing_request
            .is_some_and(|last| now.saturating_duration_since(last) < retry_interval)
        {
            return Vec::new();
        }
        let ranges = missing_ranges_for_transfer(transfer, max_ranges);
        if !ranges.is_empty() {
            transfer.last_missing_request = Some(now);
        }
        ranges
    }

    /// Collect a bounded number of idle incomplete transfers for retry after a lost terminal
    /// fragment or a contact interruption. The retry timestamp does not extend reassembly TTL.
    ///
    /// The caller must send only to authenticated, capability-advertising peers. At most
    /// `max_reports` reports and `max_ranges` ranges per report are returned per tick.
    pub fn poll_idle_missing(
        &mut self,
        now: std::time::Instant,
        min_idle: std::time::Duration,
        retry_interval: std::time::Duration,
        max_ranges: usize,
        max_reports: usize,
    ) -> Vec<MissingRequest> {
        // Each independent zero budget is a cheap exit and a separate invariant.
        if max_ranges == 0 {
            return Vec::new();
        }
        if max_reports == 0 {
            return Vec::new();
        }
        // Caller expires state through the metrics-aware reassembly gate.
        let mut candidates: Vec<_> = self
            .peers
            .iter()
            .flat_map(|(&peer, state)| {
                state.transfers.iter().filter_map(move |(&id, transfer)| {
                    let idle = now.saturating_duration_since(transfer.last_activity) >= min_idle;
                    let due = transfer
                        .last_missing_request
                        .is_none_or(|last| now.saturating_duration_since(last) >= retry_interval);
                    (idle && due).then_some((peer, id))
                })
            })
            .collect();
        // Stable ordering and a strict per-tick cap avoid an unbounded control burst.
        candidates.sort_unstable();
        let mut reports = Vec::new();
        for (peer, transfer_id) in candidates {
            if reports.len() >= max_reports {
                break;
            }
            let Some(transfer) = self
                .peers
                .get_mut(&peer)
                .and_then(|state| state.transfers.get_mut(&transfer_id))
            else {
                continue;
            };
            let ranges = missing_ranges_for_transfer(transfer, max_ranges);
            if !ranges.is_empty() {
                transfer.last_missing_request = Some(now);
                reports.push(MissingRequest {
                    peer,
                    transfer_id,
                    ranges,
                });
            }
        }
        reports
    }
}

#[cfg(test)]
mod tests;
