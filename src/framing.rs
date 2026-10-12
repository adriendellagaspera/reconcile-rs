// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;
use web_time::Instant;

use gossip::auth::{Payload, Verified};
use gossip::framing::{
    fragment_metadata, parse_recovery_control, FrameError, LogicalPayload, Reassembler,
    ReceiveOutcome, RecoveryControl, RecoveryControlError,
};
use parking_lot::Mutex;

use crate::observability;

/// What an authenticated/versioned/replay-checked application frame asks the runtime to do.
pub(crate) enum FrameEvent<'a> {
    /// One complete logical protocol payload is ready for message decoding.
    Logical {
        payload: LogicalPayload<'a>,
        /// Present only when this payload completed through fragment reassembly.
        completed_transfer: Option<[u8; 32]>,
    },
    /// Receiver has enough information to request only the currently missing byte ranges.
    RequestMissing {
        transfer_id: [u8; 32],
        ranges: Vec<(u32, u32)>,
    },
    /// Sender received one authenticated missing-range report.
    Missing {
        transfer_id: [u8; 32],
        ranges: Vec<(u32, u32)>,
    },
    /// Sender received a completion acknowledgement and may retire retained payload state.
    CompleteAck { transfer_id: [u8; 32] },
    /// No protocol-level action yet.
    Pending,
}

/// Admit one already authenticated/versioned/replay-checked application frame.
///
/// Selective-recovery control is parsed only after the common ingress gate. Ordinary complete and
/// fragment frames continue through the existing bounded reassembler. A missing report is emitted
/// on terminal arrival (rate-limited) or after an idle timeout if the terminal fragment is lost.
/// Ordinary in-flight fragments are not retried immediately.
pub(crate) fn accept_frame<'a>(
    reassembler: &Mutex<Reassembler>,
    peer: IpAddr,
    payload: Payload<'a, Verified>,
    max_missing_ranges: usize,
) -> FrameEvent<'a> {
    let bytes = payload.as_bytes();
    match parse_recovery_control(bytes, max_missing_ranges) {
        Ok(Some(RecoveryControl::Missing {
            transfer_id,
            ranges,
        })) => {
            return FrameEvent::Missing {
                transfer_id,
                ranges,
            }
        }
        Ok(Some(RecoveryControl::CompleteAck { transfer_id })) => {
            return FrameEvent::CompleteAck { transfer_id }
        }
        Ok(None) => {}
        Err(error) => {
            observability::record_reassembly_rejection(recovery_error_reason(error));
            observability::record_datagram_dropped("framing");
            return FrameEvent::Pending;
        }
    }

    let fragment = fragment_metadata(bytes);
    let mut guard = reassembler.lock();
    let report = guard.accept(peer, payload, Instant::now());

    if report.fragment_received {
        observability::record_fragment_received();
    }
    observability::record_reassembly_evictions("ttl", report.expired_transfers);
    observability::record_reassembly_evictions("capacity", report.evicted_transfers);
    observability::record_reassembly_bytes(report.retained_bytes);

    match report.outcome {
        Ok(ReceiveOutcome::Complete(payload)) => {
            if report.fragment_received {
                observability::record_reassembly_completed();
            }
            FrameEvent::Logical {
                payload,
                completed_transfer: fragment.map(|metadata| metadata.transfer_id),
            }
        }
        Ok(ReceiveOutcome::Pending) => {
            pending_fragment_event(&mut guard, peer, fragment, max_missing_ranges)
        }
        Ok(ReceiveOutcome::Duplicate) => {
            observability::record_reassembly_duplicate();
            pending_fragment_event(&mut guard, peer, fragment, max_missing_ranges)
        }
        Err(error) => {
            observability::record_reassembly_rejection(error_reason(error));
            observability::record_datagram_dropped("framing");
            FrameEvent::Pending
        }
    }
}

fn pending_fragment_event<'a>(
    reassembler: &mut Reassembler,
    peer: IpAddr,
    fragment: Option<gossip::framing::FragmentMetadata>,
    max_missing_ranges: usize,
) -> FrameEvent<'a> {
    let Some(metadata) = fragment.filter(|metadata| metadata.is_terminal()) else {
        return FrameEvent::Pending;
    };
    let ranges = reassembler.missing_ranges_if_due(
        peer,
        metadata.transfer_id,
        Instant::now(),
        std::time::Duration::from_secs(3),
        max_missing_ranges,
    );
    if ranges.is_empty() {
        FrameEvent::Pending
    } else {
        FrameEvent::RequestMissing {
            transfer_id: metadata.transfer_id,
            ranges,
        }
    }
}

/// Expire inactive fragment state even when no further datagrams arrive.
pub(crate) fn expire_reassembly(reassembler: &Mutex<Reassembler>) {
    let mut guard = reassembler.lock();
    let expired = guard.expire(Instant::now());
    let retained = guard.retained_bytes();
    drop(guard);
    observability::record_reassembly_evictions("ttl", expired);
    observability::record_reassembly_bytes(retained);
}

fn error_reason(error: FrameError) -> &'static str {
    match error {
        FrameError::Empty => "empty",
        FrameError::UnknownTag(_) => "unknown_tag",
        FrameError::TruncatedFragmentHeader => "truncated_header",
        FrameError::EmptyFragment => "empty_fragment",
        FrameError::InvalidFragmentBounds => "invalid_bounds",
        FrameError::LogicalMessageTooLarge => "logical_too_large",
        FrameError::TooManyFragments => "too_many_fragments",
        FrameError::InconsistentMetadata => "inconsistent_metadata",
        FrameError::OverlappingFragment => "overlap",
        FrameError::HashMismatch => "hash_mismatch",
        FrameError::ReassemblyCapacity => "capacity",
    }
}

fn recovery_error_reason(error: RecoveryControlError) -> &'static str {
    match error {
        RecoveryControlError::Truncated => "recovery_truncated",
        RecoveryControlError::InvalidLength => "recovery_length",
        RecoveryControlError::TooManyRanges => "recovery_ranges",
        RecoveryControlError::InvalidRange => "recovery_range",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_error_labels_are_stable_and_distinct_for_limits() {
        assert_eq!(
            error_reason(FrameError::LogicalMessageTooLarge),
            "logical_too_large"
        );
        assert_eq!(
            error_reason(FrameError::TooManyFragments),
            "too_many_fragments"
        );
        assert_eq!(error_reason(FrameError::ReassemblyCapacity), "capacity");
        assert_eq!(error_reason(FrameError::HashMismatch), "hash_mismatch");
        assert_eq!(
            recovery_error_reason(RecoveryControlError::TooManyRanges),
            "recovery_ranges"
        );
    }
}
