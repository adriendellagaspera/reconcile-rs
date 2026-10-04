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
        REASSEMBLIES_COMPLETED_TOTAL, REASSEMBLY_BYTES_CURRENT, REASSEMBLY_DUPLICATES_TOTAL,
        REASSEMBLY_EVICTIONS_TOTAL, REASSEMBLY_REJECTIONS_TOTAL,
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
}

pub(crate) use imp::*;
