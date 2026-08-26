// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Public, stable names for every metric this crate emits (behind the `metrics` feature).
//!
//! Previously each name was a `pub(crate) const` inside the internal `observability` module —
//! discoverable only by reading the source, with no stable set a dashboard or alert rule could
//! depend on (#27). This module is that stable set; `observability` records against these same
//! constants rather than defining its own.
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | [`INSERTS_TOTAL`](crate::metrics::INSERTS_TOTAL) | counter | local key insertions |
//! | [`REMOVES_TOTAL`](crate::metrics::REMOVES_TOTAL) | counter | local removals (tombstones created) |
//! | [`UPDATES_RECEIVED_TOTAL`](crate::metrics::UPDATES_RECEIVED_TOTAL) | counter | updates merged from peers |
//! | [`MESSAGES_SENT_TOTAL`](crate::metrics::MESSAGES_SENT_TOTAL) | counter | datagrams sent |
//! | [`BYTES_SENT_TOTAL`](crate::metrics::BYTES_SENT_TOTAL) | counter | wire bytes sent |
//! | [`MESSAGES_RECEIVED_TOTAL`](crate::metrics::MESSAGES_RECEIVED_TOTAL) | counter | datagrams accepted |
//! | [`BYTES_RECEIVED_TOTAL`](crate::metrics::BYTES_RECEIVED_TOTAL) | counter | wire bytes received |
//! | [`SEND_FAILURES_TOTAL`](crate::metrics::SEND_FAILURES_TOTAL) | counter | sends that exhausted all retries |
//! | [`VALUES_OVERSIZED_TOTAL`](crate::metrics::VALUES_OVERSIZED_TOTAL) | counter | single encoded messages exceeding the datagram budget, dropped on the send path — the key never converges |
//! | [`DATAGRAMS_DROPPED_TOTAL`](crate::metrics::DATAGRAMS_DROPPED_TOTAL) | counter (`reason` label) | dropped datagrams |
//! | [`ROUNDS_TOTAL`](crate::metrics::ROUNDS_TOTAL) | counter | reconciliation rounds initiated |
//! | [`TOMBSTONE_ACKS_RESENT_TOTAL`](crate::metrics::TOMBSTONE_ACKS_RESENT_TOTAL) | counter | tombstone acks resent on reconciliation rounds |
//! | [`TOMBSTONE_STAMP_BOUNDED_TOTAL`](crate::metrics::TOMBSTONE_STAMP_BOUNDED_TOTAL) | counter (`outcome` label) | tombstones whose expiry instant had to be bounded because the stored stamp led local time by more than the drift budget |
//! | [`PERSISTENCE_FAILURES_TOTAL`](crate::metrics::PERSISTENCE_FAILURES_TOTAL) | counter | snapshot writes to the persistence backend that failed |
//! | [`DISCOVERY_FAILURES_TOTAL`](crate::metrics::DISCOVERY_FAILURES_TOTAL) | counter | discovery-source resolutions that failed (round skipped) |
//! | [`ROUND_DURATION_SECONDS`](crate::metrics::ROUND_DURATION_SECONDS) | histogram | `start_reconciliation` wall time |
//! | [`HANDLE_DURATION_SECONDS`](crate::metrics::HANDLE_DURATION_SECONDS) | histogram | `handle_messages` wall time |
//! | [`PEERS_CURRENT`](crate::metrics::PEERS_CURRENT) | gauge | size of the gossip-routing peer set right now |
//! | [`MEMBERS_CURRENT`](crate::metrics::MEMBERS_CURRENT) | gauge | size of the causal-stability membership set right now |
//! | [`ENTRIES_CURRENT`](crate::metrics::ENTRIES_CURRENT) | gauge | live (non-tombstone) entries right now |
//! | [`TOMBSTONES_CURRENT`](crate::metrics::TOMBSTONES_CURRENT) | gauge | outstanding tombstones right now |
//! | [`BULK_DUMPS_IN_FLIGHT`](crate::metrics::BULK_DUMPS_IN_FLIGHT) | gauge | bulk anti-entropy dumps in flight right now |
//! | [`PERSISTENCE_FAILURES_CURRENT`](crate::metrics::PERSISTENCE_FAILURES_CURRENT) | gauge | consecutive snapshot failures since the last success (0 when healthy) |

/// Local key insertions.
pub const INSERTS_TOTAL: &str = "reconcile_inserts_total";
/// Local removals (tombstones created).
pub const REMOVES_TOTAL: &str = "reconcile_removes_total";
/// Updates merged from peers.
pub const UPDATES_RECEIVED_TOTAL: &str = "reconcile_updates_received_total";
/// Datagrams sent.
pub const MESSAGES_SENT_TOTAL: &str = "reconcile_messages_sent_total";
/// Wire bytes sent.
pub const BYTES_SENT_TOTAL: &str = "reconcile_bytes_sent_total";
/// Datagrams accepted.
pub const MESSAGES_RECEIVED_TOTAL: &str = "reconcile_messages_received_total";
/// Wire bytes received.
pub const BYTES_RECEIVED_TOTAL: &str = "reconcile_bytes_received_total";
/// Sends that exhausted all retries.
pub const SEND_FAILURES_TOTAL: &str = "reconcile_send_failures_total";
/// Single encoded messages exceeding the datagram budget, dropped on the send path — the key
/// never converges.
pub const VALUES_OVERSIZED_TOTAL: &str = "reconcile_values_oversized_total";
/// Dropped datagrams, labeled `reason`.
pub const DATAGRAMS_DROPPED_TOTAL: &str = "reconcile_datagrams_dropped_total";
/// Reconciliation rounds initiated.
pub const ROUNDS_TOTAL: &str = "reconcile_rounds_total";
/// Tombstone acks resent on reconciliation rounds.
pub const TOMBSTONE_ACKS_RESENT_TOTAL: &str = "reconcile_tombstone_acks_resent_total";
/// Tombstones whose expiry instant had to be bounded, labeled `outcome`.
pub const TOMBSTONE_STAMP_BOUNDED_TOTAL: &str = "reconcile_tombstone_stamp_bounded_total";
/// Snapshot writes to the persistence backend that failed — both the periodic background
/// snapshot and a caller-triggered [`snapshot_now`](crate::ReplicatedMap::snapshot_now).
pub const PERSISTENCE_FAILURES_TOTAL: &str = "reconcile_persistence_failures_total";
/// Discovery-source resolutions that failed; the round is skipped, membership untouched.
pub const DISCOVERY_FAILURES_TOTAL: &str = "reconcile_discovery_failures_total";
/// Duration of `start_reconciliation`.
pub const ROUND_DURATION_SECONDS: &str = "reconcile_round_duration_seconds";
/// Duration of `handle_messages`.
pub const HANDLE_DURATION_SECONDS: &str = "reconcile_handle_messages_duration_seconds";
/// Current size of the gossip-routing peer set — see
/// [`ReplicatedMap::peers`](crate::ReplicatedMap::peers).
pub const PEERS_CURRENT: &str = "reconcile_peers_current";
/// Current size of the causal-stability membership set — see
/// [`ReplicatedMap::members`](crate::ReplicatedMap::members).
pub const MEMBERS_CURRENT: &str = "reconcile_members_current";
/// Current count of live (non-tombstone) entries — see
/// [`ReplicatedMap::len`](crate::ReplicatedMap::len).
pub const ENTRIES_CURRENT: &str = "reconcile_entries_current";
/// Current count of outstanding tombstones (deleted keys not yet garbage-collected).
pub const TOMBSTONES_CURRENT: &str = "reconcile_tombstones_current";
/// Current count of bulk anti-entropy dumps in flight, across all peers (bounded by
/// `Config::max_concurrent_bulk_dumps`).
pub const BULK_DUMPS_IN_FLIGHT: &str = "reconcile_bulk_dumps_in_flight";
/// Consecutive persistence-backend snapshot failures since the last success; `0` while healthy.
/// A sustained non-zero value means durability has been broken since it last rose from zero.
pub const PERSISTENCE_FAILURES_CURRENT: &str = "reconcile_persistence_failures_current";
