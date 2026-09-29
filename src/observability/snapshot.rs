// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[cfg(feature = "metrics")]
mod imp {
    use std::time::Instant;

    use ::metrics::{counter, gauge, histogram};

    use crate::metrics::*;

    #[inline]
    pub(crate) fn record_snapshot_full(compaction: bool, segments: usize, bytes: u64) {
        counter!(SNAPSHOT_FULL_MATERIALIZATIONS_TOTAL).increment(1);
        if compaction {
            counter!(SNAPSHOT_COMPACTIONS_TOTAL).increment(1);
        }
        gauge!(SNAPSHOT_SEGMENTS_CURRENT).set(segments as f64);
        gauge!(SNAPSHOT_SEGMENT_BYTES_CURRENT).set(bytes as f64);
    }

    #[inline]
    pub(crate) fn record_snapshot_delta(segments: usize, bytes: u64) {
        counter!(SNAPSHOT_DELTA_COMMITS_TOTAL).increment(1);
        gauge!(SNAPSHOT_SEGMENTS_CURRENT).set(segments as f64);
        gauge!(SNAPSHOT_SEGMENT_BYTES_CURRENT).set(bytes as f64);
    }

    #[inline]
    pub(crate) fn record_snapshot_cleanup_failure() {
        counter!(SNAPSHOT_CLEANUP_FAILURES_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_snapshot_recovery(start: Option<Instant>, segments: usize) {
        gauge!(SNAPSHOT_RECOVERY_SEGMENTS).set(segments as f64);
        if let Some(start) = start {
            histogram!(SNAPSHOT_RECOVERY_DURATION_SECONDS).record(start.elapsed().as_secs_f64());
        }
    }

    #[cfg(feature = "metrics-prometheus")]
    pub(crate) fn describe() {
        use ::metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(
            SNAPSHOT_FULL_MATERIALIZATIONS_TOTAL,
            Unit::Count,
            "Full FileSnapshot base publications"
        );
        describe_counter!(
            SNAPSHOT_DELTA_COMMITS_TOTAL,
            Unit::Count,
            "Incremental FileSnapshot delta publications"
        );
        describe_counter!(
            SNAPSHOT_COMPACTIONS_TOTAL,
            Unit::Count,
            "Full base publications replacing an existing delta chain"
        );
        describe_counter!(
            SNAPSHOT_CLEANUP_FAILURES_TOTAL,
            Unit::Count,
            "Failed removal of superseded FileSnapshot files after manifest publication"
        );
        describe_gauge!(
            SNAPSHOT_SEGMENTS_CURRENT,
            Unit::Count,
            "Currently committed FileSnapshot base plus delta segment count"
        );
        describe_gauge!(
            SNAPSHOT_SEGMENT_BYTES_CURRENT,
            Unit::Bytes,
            "Bytes retained by committed FileSnapshot base and delta segments"
        );
        describe_gauge!(
            SNAPSHOT_RECOVERY_SEGMENTS,
            Unit::Count,
            "FileSnapshot segments replayed by the latest successful load"
        );
        describe_histogram!(
            SNAPSHOT_RECOVERY_DURATION_SECONDS,
            Unit::Seconds,
            "FileSnapshot recovery wall time"
        );
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    use std::time::Instant;

    #[inline(always)]
    pub(crate) fn record_snapshot_full(_compaction: bool, _segments: usize, _bytes: u64) {}

    #[inline(always)]
    pub(crate) fn record_snapshot_delta(_segments: usize, _bytes: u64) {}

    #[inline(always)]
    pub(crate) fn record_snapshot_cleanup_failure() {}

    #[inline(always)]
    pub(crate) fn record_snapshot_recovery(_start: Option<Instant>, _segments: usize) {}
}

pub(crate) use imp::{
    record_snapshot_cleanup_failure, record_snapshot_delta, record_snapshot_full,
    record_snapshot_recovery,
};

#[cfg(feature = "metrics-prometheus")]
pub(crate) fn describe_snapshot_metrics() {
    imp::describe();
}
