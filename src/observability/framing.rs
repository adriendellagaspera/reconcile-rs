// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(feature = "metrics")]
mod imp {
    use ::metrics::{counter, gauge};

    use crate::metrics::{
        FRAGMENTED_MESSAGES_TOTAL, FRAGMENTS_RECEIVED_TOTAL, FRAGMENTS_SENT_TOTAL,
        OUTBOUND_RECOVERY_BYTES_CURRENT, OUTBOUND_RECOVERY_EVICTIONS_TOTAL,
        OUTBOUND_RECOVERY_TRANSFERS_CURRENT, REASSEMBLIES_COMPLETED_TOTAL,
        REASSEMBLY_BYTES_CURRENT, REASSEMBLY_DUPLICATES_TOTAL, REASSEMBLY_EVICTIONS_TOTAL,
        REASSEMBLY_REJECTIONS_TOTAL, SELECTIVE_RECOVERY_COMPLETION_ACKS_TOTAL,
        SELECTIVE_RECOVERY_CONTROL_BYTES_TOTAL, SELECTIVE_RECOVERY_FALLBACKS_TOTAL,
        SELECTIVE_RECOVERY_MISSING_RANGES_TOTAL, SELECTIVE_RECOVERY_REQUESTS_TOTAL,
        SELECTIVE_RECOVERY_RETRANSMITTED_BYTES_TOTAL,
        SELECTIVE_RECOVERY_RETRANSMITTED_FRAGMENTS_TOTAL,
    };

    #[inline]
    pub(crate) fn record_fragmented_message() {
        counter!(FRAGMENTED_MESSAGES_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_fragment_sent() {
        counter!(FRAGMENTS_SENT_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_fragment_received() {
        counter!(FRAGMENTS_RECEIVED_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_reassembly_completed() {
        counter!(REASSEMBLIES_COMPLETED_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_reassembly_duplicate() {
        counter!(REASSEMBLY_DUPLICATES_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_reassembly_evictions(reason: &'static str, count: usize) {
        if count > 0 {
            counter!(REASSEMBLY_EVICTIONS_TOTAL, "reason" => reason).increment(count as u64);
        }
    }

    #[inline]
    pub(crate) fn record_reassembly_rejection(reason: &'static str) {
        counter!(REASSEMBLY_REJECTIONS_TOTAL, "reason" => reason).increment(1);
    }

    #[inline]
    pub(crate) fn record_reassembly_bytes(bytes: usize) {
        gauge!(REASSEMBLY_BYTES_CURRENT).set(bytes as f64);
    }

    #[inline]
    pub(crate) fn record_selective_recovery_request(ranges: usize, wire_bytes: usize) {
        counter!(SELECTIVE_RECOVERY_REQUESTS_TOTAL).increment(1);
        counter!(SELECTIVE_RECOVERY_MISSING_RANGES_TOTAL).increment(ranges as u64);
        counter!(SELECTIVE_RECOVERY_CONTROL_BYTES_TOTAL).increment(wire_bytes as u64);
    }

    #[inline]
    pub(crate) fn record_selective_retransmit(payload_bytes: usize) {
        counter!(SELECTIVE_RECOVERY_RETRANSMITTED_FRAGMENTS_TOTAL).increment(1);
        counter!(SELECTIVE_RECOVERY_RETRANSMITTED_BYTES_TOTAL).increment(payload_bytes as u64);
    }

    #[inline]
    pub(crate) fn record_completion_ack(wire_bytes: usize) {
        counter!(SELECTIVE_RECOVERY_COMPLETION_ACKS_TOTAL).increment(1);
        counter!(SELECTIVE_RECOVERY_CONTROL_BYTES_TOTAL).increment(wire_bytes as u64);
    }

    #[inline]
    pub(crate) fn record_selective_recovery_fallback(reason: &'static str) {
        counter!(SELECTIVE_RECOVERY_FALLBACKS_TOTAL, "reason" => reason).increment(1);
    }

    #[inline]
    pub(crate) fn record_outbound_recovery_evictions(reason: &'static str, count: usize) {
        if count > 0 {
            counter!(OUTBOUND_RECOVERY_EVICTIONS_TOTAL, "reason" => reason).increment(count as u64);
        }
    }

    #[inline]
    pub(crate) fn record_outbound_recovery_state(transfers: usize, bytes: usize) {
        gauge!(OUTBOUND_RECOVERY_TRANSFERS_CURRENT).set(transfers as f64);
        gauge!(OUTBOUND_RECOVERY_BYTES_CURRENT).set(bytes as f64);
    }

    #[cfg(feature = "metrics-prometheus")]
    pub(crate) fn describe() {
        use ::metrics::{describe_counter, describe_gauge, Unit};

        describe_counter!(
            FRAGMENTED_MESSAGES_TOTAL,
            Unit::Count,
            "Logical protocol messages split across fragment frames"
        );
        describe_counter!(FRAGMENTS_SENT_TOTAL, Unit::Count, "Fragment datagrams sent");
        describe_counter!(
            FRAGMENTS_RECEIVED_TOTAL,
            Unit::Count,
            "Authenticated fragment datagrams received"
        );
        describe_counter!(
            REASSEMBLIES_COMPLETED_TOTAL,
            Unit::Count,
            "Fragmented logical messages successfully reassembled"
        );
        describe_counter!(
            REASSEMBLY_DUPLICATES_TOTAL,
            Unit::Count,
            "Exact duplicate fragments reused"
        );
        describe_counter!(
            REASSEMBLY_EVICTIONS_TOTAL,
            Unit::Count,
            "Incomplete transfers removed by ttl or capacity eviction"
        );
        describe_counter!(
            REASSEMBLY_REJECTIONS_TOTAL,
            Unit::Count,
            "Authenticated framing/reassembly rejections by reason"
        );
        describe_gauge!(
            REASSEMBLY_BYTES_CURRENT,
            Unit::Bytes,
            "Bytes retained for incomplete fragment reassembly"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_REQUESTS_TOTAL,
            Unit::Count,
            "Authenticated missing-range recovery reports sent"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_MISSING_RANGES_TOTAL,
            Unit::Count,
            "Missing byte ranges requested"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_RETRANSMITTED_FRAGMENTS_TOTAL,
            Unit::Count,
            "Selectively retransmitted fragment datagrams"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_RETRANSMITTED_BYTES_TOTAL,
            Unit::Bytes,
            "Selectively retransmitted logical payload bytes"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_CONTROL_BYTES_TOTAL,
            Unit::Bytes,
            "Wire bytes spent on selective-recovery control frames"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_COMPLETION_ACKS_TOTAL,
            Unit::Count,
            "Completion acknowledgements sent after fragmented reassembly"
        );
        describe_counter!(
            SELECTIVE_RECOVERY_FALLBACKS_TOTAL,
            Unit::Count,
            "Selective recovery fallbacks by reason"
        );
        describe_gauge!(
            OUTBOUND_RECOVERY_TRANSFERS_CURRENT,
            Unit::Count,
            "Retained outbound recoverable transfers"
        );
        describe_gauge!(
            OUTBOUND_RECOVERY_BYTES_CURRENT,
            Unit::Bytes,
            "Retained outbound recovery payload bytes"
        );
        describe_counter!(
            OUTBOUND_RECOVERY_EVICTIONS_TOTAL,
            Unit::Count,
            "Outbound recovery state removed by ttl or capacity"
        );
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    #[inline(always)]
    pub(crate) fn record_fragmented_message() {}

    #[inline(always)]
    pub(crate) fn record_fragment_sent() {}

    #[inline(always)]
    pub(crate) fn record_fragment_received() {}

    #[inline(always)]
    pub(crate) fn record_reassembly_completed() {}

    #[inline(always)]
    pub(crate) fn record_reassembly_duplicate() {}

    #[inline(always)]
    pub(crate) fn record_reassembly_evictions(_reason: &'static str, _count: usize) {}

    #[inline(always)]
    pub(crate) fn record_reassembly_rejection(_reason: &'static str) {}

    #[inline(always)]
    pub(crate) fn record_reassembly_bytes(_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_selective_recovery_request(_ranges: usize, _wire_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_selective_retransmit(_payload_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_completion_ack(_wire_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_selective_recovery_fallback(_reason: &'static str) {}

    #[inline(always)]
    pub(crate) fn record_outbound_recovery_evictions(_reason: &'static str, _count: usize) {}

    #[inline(always)]
    pub(crate) fn record_outbound_recovery_state(_transfers: usize, _bytes: usize) {}
}

pub(crate) use imp::*;

#[cfg(all(test, feature = "metrics"))]
mod tests;
