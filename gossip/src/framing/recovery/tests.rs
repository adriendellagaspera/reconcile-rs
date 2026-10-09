// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

#[test]
fn missing_report_round_trips_and_rejects_overlap() {
    let id = [7u8; 32];
    let mut frame = Vec::new();
    write_missing_report(id, &[(0, 10), (20, 5)], &mut frame).unwrap();
    assert_eq!(
        parse_recovery_control(&frame, 2).unwrap(),
        Some(RecoveryControl::Missing {
            transfer_id: id,
            ranges: vec![(0, 10), (20, 5)],
        })
    );
    assert_eq!(
        parse_recovery_control(&frame, 1),
        Err(RecoveryControlError::TooManyRanges)
    );
    assert_eq!(
        write_missing_report(id, &[(10, 10), (15, 2)], &mut frame),
        Err(RecoveryControlError::InvalidRange)
    );
}

#[test]
fn incomplete_transfer_without_terminal_reports_missing_after_idle_and_survives_gap() {
    use std::collections::{BTreeMap, HashMap};
    use std::time::{Duration, Instant};

    let start = Instant::now();
    let peer: IpAddr = "127.0.0.81".parse().unwrap();
    let transfer_id = [19; 32];
    let limits = super::super::ReassemblyLimits {
        max_logical_message_size: 1_024,
        max_fragments_per_message: 32,
        max_incomplete_transfers_per_peer: 4,
        max_reassembly_bytes_per_peer: 1_024,
        max_total_reassembly_bytes: 4_096,
        reassembly_ttl: Duration::from_secs(90),
    };
    let mut reassembler = Reassembler::new(limits);
    // The first 80% arrived, but the terminal fragment was lost before interruption.
    reassembler.peers.insert(
        peer,
        super::super::PeerState {
            transfers: HashMap::from([(
                transfer_id,
                super::super::Transfer {
                    total_len: 10,
                    fragments: BTreeMap::from([(0, vec![1; 8])]),
                    retained_bytes: 8,
                    last_activity: start,
                    last_missing_request: None,
                },
            )]),
            retained_bytes: 8,
        },
    );
    reassembler.total_retained_bytes = 8;
    // Do not request prematurely during an ordinary in-flight transfer.
    assert!(reassembler
        .poll_idle_missing(
            start + Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(3),
            8,
            1,
        )
        .is_empty());
    let after_gap = start + Duration::from_secs(30);
    let requests = reassembler.poll_idle_missing(
        after_gap,
        Duration::from_secs(1),
        Duration::from_secs(3),
        8,
        1,
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].transfer_id, transfer_id);
    assert_eq!(requests[0].ranges, vec![(8, 2)]);
    assert_eq!(reassembler.retained_bytes(), 8);
    // Control retransmission is bounded and never refreshes the retained-data TTL.
    assert!(reassembler
        .poll_idle_missing(
            after_gap + Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(3),
            8,
            1,
        )
        .is_empty());
    assert_eq!(
        reassembler
            .poll_idle_missing(
                after_gap + Duration::from_secs(4),
                Duration::from_secs(1),
                Duration::from_secs(3),
                8,
                1,
            )
            .len(),
        1
    );
    assert_eq!(reassembler.expire(start + Duration::from_secs(90)), 1);
    assert!(reassembler
        .poll_idle_missing(
            start + Duration::from_secs(90),
            Duration::from_secs(1),
            Duration::from_secs(3),
            8,
            1,
        )
        .is_empty());
    assert_eq!(reassembler.retained_bytes(), 0);
}

#[test]
fn completion_ack_round_trips() {
    let id = [9u8; 32];
    let mut frame = Vec::new();
    write_completion_ack(id, &mut frame);
    assert_eq!(
        parse_recovery_control(&frame, 0).unwrap(),
        Some(RecoveryControl::CompleteAck { transfer_id: id })
    );
}

#[test]
fn budget_caps_missing_range_count() {
    assert_eq!(
        max_missing_ranges_for_budget(MISSING_REPORT_HEADER_LEN + 3 * MISSING_RANGE_LEN, 0, 99),
        3
    );
    assert_eq!(
        max_missing_ranges_for_budget(MISSING_REPORT_HEADER_LEN, 0, 99),
        0
    );
}
