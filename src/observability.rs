// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Centralized observability helpers: the one place the `#[cfg(feature = "metrics")]` gate lives.
//! Each helper is an `#[inline]` no-op when the feature is off.
//!
//! Metric names are public, stable constants in [`crate::metrics`] (#27) — this module only
//! records against them.

#[cfg(feature = "metrics")]
mod imp {
    use std::time::Instant;

    // `::` prefix: this crate also declares a local `metrics` module (`crate::metrics`, the
    // public metric-name constants below pull in), which would otherwise shadow the `metrics`
    // dependency crate for every unqualified `metrics::` path in the crate.
    use ::metrics::{counter, gauge, histogram};

    use crate::metrics::*;

    /// Start a latency-histogram timer; `None` when the `metrics` feature is off.
    #[inline]
    pub(crate) fn timer() -> Option<Instant> {
        Some(Instant::now())
    }

    #[inline]
    pub(crate) fn record_insert() {
        counter!(INSERTS_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_remove() {
        counter!(REMOVES_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_updates_received(n: usize) {
        counter!(UPDATES_RECEIVED_TOTAL).increment(n as u64);
    }

    #[inline]
    pub(crate) fn record_bytes_sent(bytes: usize) {
        counter!(MESSAGES_SENT_TOTAL).increment(1);
        counter!(BYTES_SENT_TOTAL).increment(bytes as u64);
    }

    #[inline]
    pub(crate) fn record_bytes_received(bytes: usize) {
        counter!(MESSAGES_RECEIVED_TOTAL).increment(1);
        counter!(BYTES_RECEIVED_TOTAL).increment(bytes as u64);
    }

    #[inline]
    pub(crate) fn record_send_failure() {
        counter!(SEND_FAILURES_TOTAL).increment(1);
    }

    /// A single message's encoded size exceeds the datagram budget on its own — dropped, never
    /// sent. Distinct from [`record_send_failure`] (a transport-level failure to send a
    /// well-formed datagram): this is a structurally undeliverable message, alertable on its own.
    #[inline]
    pub(crate) fn record_value_oversized() {
        counter!(VALUES_OVERSIZED_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_datagram_dropped(reason: &'static str) {
        counter!(DATAGRAMS_DROPPED_TOTAL, "reason" => reason).increment(1);
    }

    /// A write hit the `max_concurrent_broadcasts` egress budget (#83). `path` is `"eager"`
    /// (`insert`/`update`/`insert_bulk`: the write applied, only its broadcast was skipped) or
    /// `"try"` (`try_insert`/`try_update`: the whole call was rejected).
    #[inline]
    pub(crate) fn record_broadcast_backpressure(path: &'static str) {
        counter!(BROADCAST_BACKPRESSURE_TOTAL, "path" => path).increment(1);
    }

    #[inline]
    pub(crate) fn record_reconcile_round() {
        counter!(ROUNDS_TOTAL).increment(1);
    }

    #[inline]
    pub(crate) fn record_tombstone_acks_resent(n: usize) {
        counter!(TOMBSTONE_ACKS_RESENT_TOTAL).increment(n as u64);
    }

    /// A tombstone's expiry instant had to be bounded. A non-zero rate means a peer is planting
    /// stamps far ahead of this node's clock.
    #[inline]
    pub(crate) fn record_tombstone_stamp_bounded(outcome: &'static str) {
        counter!(TOMBSTONE_STAMP_BOUNDED_TOTAL, "outcome" => outcome).increment(1);
    }

    #[inline]
    pub(crate) fn record_round_duration(start: Option<Instant>) {
        if let Some(start) = start {
            histogram!(ROUND_DURATION_SECONDS).record(start.elapsed().as_secs_f64());
        }
    }

    #[inline]
    pub(crate) fn record_handle_duration(start: Option<Instant>) {
        if let Some(start) = start {
            histogram!(HANDLE_DURATION_SECONDS).record(start.elapsed().as_secs_f64());
        }
    }

    /// A snapshot write to the persistence backend failed. `consecutive` is the count of
    /// consecutive failures since the last success (this one included), driving
    /// [`PERSISTENCE_FAILURES_CURRENT`] — a sustained non-zero gauge is durability broken for the
    /// process lifetime while the node otherwise looks healthy.
    #[inline]
    pub(crate) fn record_persistence_failure(consecutive: usize) {
        counter!(PERSISTENCE_FAILURES_TOTAL).increment(1);
        gauge!(PERSISTENCE_FAILURES_CURRENT).set(consecutive as f64);
    }

    /// A snapshot write to the persistence backend succeeded, resetting the consecutive-failure
    /// gauge.
    #[inline]
    pub(crate) fn record_persistence_success() {
        gauge!(PERSISTENCE_FAILURES_CURRENT).set(0.0);
    }

    /// A discovery-source resolution failed; the round is skipped, membership untouched.
    #[inline]
    pub(crate) fn record_discovery_failure() {
        counter!(DISCOVERY_FAILURES_TOTAL).increment(1);
    }

    /// Refresh the "what is the state now" gauges, once per reconciliation round
    /// ([`Replica::start_reconciliation`](crate::replica::Replica::start_reconciliation)) rather
    /// than at every mutation — cheap enough at that cadence, and a gauge scraped periodically
    /// gains nothing from being updated more often than that.
    #[inline]
    pub(crate) fn record_state_gauges(
        peers: usize,
        members: usize,
        entries: usize,
        tombstones: usize,
        bulk_dumps_in_flight: usize,
        broadcasts_in_flight: usize,
    ) {
        gauge!(PEERS_CURRENT).set(peers as f64);
        gauge!(MEMBERS_CURRENT).set(members as f64);
        gauge!(ENTRIES_CURRENT).set(entries as f64);
        gauge!(TOMBSTONES_CURRENT).set(tombstones as f64);
        gauge!(BULK_DUMPS_IN_FLIGHT).set(bulk_dumps_in_flight as f64);
        gauge!(BROADCASTS_IN_FLIGHT).set(broadcasts_in_flight as f64);
    }

    /// Register descriptions and units for all metrics. Idempotent; call after installing a
    /// recorder.
    #[cfg(feature = "metrics-prometheus")]
    pub(crate) fn describe() {
        use ::metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(INSERTS_TOTAL, Unit::Count, "Local key insertions");
        describe_counter!(
            REMOVES_TOTAL,
            Unit::Count,
            "Local removals (tombstones created)"
        );
        describe_counter!(
            UPDATES_RECEIVED_TOTAL,
            Unit::Count,
            "Updates merged from peers"
        );
        describe_counter!(MESSAGES_SENT_TOTAL, Unit::Count, "Datagrams sent");
        describe_counter!(BYTES_SENT_TOTAL, Unit::Bytes, "Wire bytes sent");
        describe_counter!(MESSAGES_RECEIVED_TOTAL, Unit::Count, "Datagrams accepted");
        describe_counter!(BYTES_RECEIVED_TOTAL, Unit::Bytes, "Wire bytes received");
        describe_counter!(
            SEND_FAILURES_TOTAL,
            Unit::Count,
            "Sends that exhausted all retries"
        );
        describe_counter!(
            VALUES_OVERSIZED_TOTAL,
            Unit::Count,
            "Single encoded messages exceeding the datagram budget, dropped on the send path"
        );
        describe_counter!(
            DATAGRAMS_DROPPED_TOTAL,
            Unit::Count,
            "Datagrams dropped, by reason"
        );
        describe_counter!(
            BROADCAST_BACKPRESSURE_TOTAL,
            Unit::Count,
            "Writes that hit the write-broadcast egress budget, by path"
        );
        describe_counter!(ROUNDS_TOTAL, Unit::Count, "Reconciliation rounds initiated");
        describe_counter!(
            TOMBSTONE_ACKS_RESENT_TOTAL,
            Unit::Count,
            "Tombstone acks resent on reconciliation rounds"
        );
        describe_counter!(
            TOMBSTONE_STAMP_BOUNDED_TOTAL,
            Unit::Count,
            "Tombstones whose expiry instant had to be bounded, by outcome"
        );
        describe_histogram!(
            ROUND_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of start_reconciliation"
        );
        describe_histogram!(
            HANDLE_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of handle_messages"
        );
        describe_counter!(
            PERSISTENCE_FAILURES_TOTAL,
            Unit::Count,
            "Snapshot writes to the persistence backend that failed"
        );
        describe_counter!(
            DISCOVERY_FAILURES_TOTAL,
            Unit::Count,
            "Discovery-source resolutions that failed"
        );
        describe_gauge!(
            PEERS_CURRENT,
            Unit::Count,
            "Current size of the gossip-routing peer set"
        );
        describe_gauge!(
            MEMBERS_CURRENT,
            Unit::Count,
            "Current size of the causal-stability membership set"
        );
        describe_gauge!(
            ENTRIES_CURRENT,
            Unit::Count,
            "Current count of live (non-tombstone) entries"
        );
        describe_gauge!(
            TOMBSTONES_CURRENT,
            Unit::Count,
            "Current count of outstanding tombstones"
        );
        describe_gauge!(
            BULK_DUMPS_IN_FLIGHT,
            Unit::Count,
            "Current count of bulk anti-entropy dumps in flight"
        );
        describe_gauge!(
            BROADCASTS_IN_FLIGHT,
            Unit::Count,
            "Current count of write-broadcast tasks in flight"
        );
        describe_gauge!(
            PERSISTENCE_FAILURES_CURRENT,
            Unit::Count,
            "Consecutive persistence-backend snapshot failures since the last success"
        );
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    use std::time::Instant;

    #[inline(always)]
    pub(crate) fn timer() -> Option<Instant> {
        None
    }

    #[inline(always)]
    pub(crate) fn record_insert() {}

    #[inline(always)]
    pub(crate) fn record_remove() {}

    #[inline(always)]
    pub(crate) fn record_updates_received(_n: usize) {}

    #[inline(always)]
    pub(crate) fn record_bytes_sent(_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_bytes_received(_bytes: usize) {}

    #[inline(always)]
    pub(crate) fn record_send_failure() {}

    #[inline(always)]
    pub(crate) fn record_value_oversized() {}

    #[inline(always)]
    pub(crate) fn record_datagram_dropped(_reason: &'static str) {}

    #[inline(always)]
    pub(crate) fn record_broadcast_backpressure(_path: &'static str) {}

    #[inline(always)]
    pub(crate) fn record_reconcile_round() {}

    #[inline(always)]
    pub(crate) fn record_tombstone_acks_resent(_n: usize) {}

    #[inline(always)]
    pub(crate) fn record_tombstone_stamp_bounded(_outcome: &'static str) {}

    #[inline(always)]
    pub(crate) fn record_round_duration(_start: Option<Instant>) {}

    #[inline(always)]
    pub(crate) fn record_handle_duration(_start: Option<Instant>) {}

    #[inline(always)]
    pub(crate) fn record_persistence_failure(_consecutive: usize) {}

    #[inline(always)]
    pub(crate) fn record_persistence_success() {}

    #[inline(always)]
    pub(crate) fn record_discovery_failure() {}

    #[inline(always)]
    pub(crate) fn record_state_gauges(
        _peers: usize,
        _members: usize,
        _entries: usize,
        _tombstones: usize,
        _bulk_dumps_in_flight: usize,
        _broadcasts_in_flight: usize,
    ) {
    }
}

pub(crate) use imp::*;
