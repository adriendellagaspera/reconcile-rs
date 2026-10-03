// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;
use std::time::Instant;

use gossip::auth::{Payload, Verified};
use gossip::framing::{FrameError, LogicalPayload, Reassembler, ReceiveOutcome};
use parking_lot::Mutex;

use crate::observability;

/// Admit one already authenticated/versioned/replay-checked application frame.
///
/// Incomplete and duplicate fragments deliberately return None: callers must not clear a pending
/// repair until a complete logical protocol payload exists. A retry then re-emits the same
/// content-addressed fragments and reuses retained progress.
pub(crate) fn accept_frame<'a>(
    reassembler: &Mutex<Reassembler>,
    peer: IpAddr,
    payload: Payload<'a, Verified>,
) -> Option<LogicalPayload<'a>> {
    let report = reassembler.lock().accept(peer, payload, Instant::now());

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
            Some(payload)
        }
        Ok(ReceiveOutcome::Pending) => None,
        Ok(ReceiveOutcome::Duplicate) => {
            observability::record_reassembly_duplicate();
            None
        }
        Err(error) => {
            observability::record_reassembly_rejection(error_reason(error));
            observability::record_datagram_dropped("framing");
            None
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_error_labels_are_stable_and_distinct_for_limits() {
        assert_eq!(error_reason(FrameError::LogicalMessageTooLarge), "logical_too_large");
        assert_eq!(error_reason(FrameError::TooManyFragments), "too_many_fragments");
        assert_eq!(error_reason(FrameError::ReassemblyCapacity), "capacity");
        assert_eq!(error_reason(FrameError::HashMismatch), "hash_mismatch");
    }
}
