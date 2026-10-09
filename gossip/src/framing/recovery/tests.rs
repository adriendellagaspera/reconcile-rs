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
fn empty_missing_report_is_valid_at_exact_header_length() {
    let id = [0x68; 32];
    let mut frame = Vec::new();
    write_missing_report(id, &[], &mut frame).unwrap();
    assert_eq!(frame.len(), MISSING_REPORT_HEADER_LEN);
    assert_eq!(
        parse_recovery_control(&frame, 0).unwrap(),
        Some(RecoveryControl::Missing {
            transfer_id: id,
            ranges: vec![],
        })
    );
    assert_eq!(
        parse_recovery_control(&frame[..MISSING_REPORT_HEADER_LEN - 1], 0),
        Err(RecoveryControlError::Truncated)
    );
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

fn seeded_reassembler(
    peer: IpAddr,
    id: [u8; 32],
    total_len: usize,
    fragments: &[(usize, &[u8])],
    now: std::time::Instant,
) -> Reassembler {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    let retained = fragments.iter().map(|(_, bytes)| bytes.len()).sum();
    let mut reassembler = Reassembler::new(super::super::ReassemblyLimits {
        max_logical_message_size: 1024,
        max_fragments_per_message: 64,
        max_incomplete_transfers_per_peer: 4,
        max_reassembly_bytes_per_peer: 1024,
        max_total_reassembly_bytes: 4096,
        reassembly_ttl: Duration::from_secs(90),
    });
    reassembler.peers.insert(
        peer,
        super::super::PeerState {
            transfers: HashMap::from([(
                id,
                super::super::Transfer {
                    total_len,
                    fragments: fragments
                        .iter()
                        .map(|(offset, bytes)| (*offset, bytes.to_vec()))
                        .collect::<BTreeMap<_, _>>(),
                    retained_bytes: retained,
                    last_activity: now,
                    last_missing_request: None,
                },
            )]),
            retained_bytes: retained,
        },
    );
    reassembler.total_retained_bytes = retained;
    reassembler
}

#[test]
fn fragment_metadata_requires_real_fragment_and_preserves_exact_offsets() {
    let id = [0x56; 32];
    assert_eq!(fragment_metadata(&[]), None);
    assert_eq!(fragment_metadata(&[0]), None);
    let mut frame = Vec::new();
    super::super::write_fragment(id, 10, 7, b"xyz", &mut frame).unwrap();
    let metadata = fragment_metadata(&frame).unwrap();
    assert_eq!(metadata.transfer_id, id);
    assert_eq!(metadata.total_len, 10);
    assert_eq!(metadata.offset, 7);
    assert_eq!(metadata.payload_len, 3);
    assert!(metadata.is_terminal());

    let mut not_terminal = frame.clone();
    // The offset field is at 37..41, and an earlier fragment must not be terminal.
    not_terminal[37..41].copy_from_slice(&2u32.to_le_bytes());
    let metadata = fragment_metadata(&not_terminal).unwrap();
    assert_eq!(metadata.offset, 2);
    assert!(!metadata.is_terminal());

    assert_eq!(fragment_metadata(&frame[..FRAGMENT_HEADER_LEN]), None);
    assert_eq!(fragment_metadata(&frame[..FRAGMENT_HEADER_LEN - 1]), None);
    frame[0] = 0;
    assert_eq!(fragment_metadata(&frame), None);
}

#[test]
fn missing_ranges_preserve_exact_gaps_and_are_bounded_by_count() {
    let now = std::time::Instant::now();
    let peer: IpAddr = "127.0.0.91".parse().unwrap();
    let id = [0x61; 32];
    let receiver = seeded_reassembler(peer, id, 11, &[(2, b"ab"), (6, b"cd")], now);
    assert_eq!(receiver.missing_ranges(peer, id, 0), vec![]);
    assert_eq!(receiver.missing_ranges(peer, [0; 32], 3), vec![]);
    assert_eq!(
        receiver.missing_ranges("127.0.0.92".parse().unwrap(), id, 3),
        vec![]
    );
    assert_eq!(receiver.missing_ranges(peer, id, 1), vec![(0, 2)]);
    assert_eq!(receiver.missing_ranges(peer, id, 2), vec![(0, 2), (4, 2)]);
    assert_eq!(
        receiver.missing_ranges(peer, id, 3),
        vec![(0, 2), (4, 2), (8, 3)]
    );
    assert_eq!(
        receiver.missing_ranges(peer, id, 8),
        vec![(0, 2), (4, 2), (8, 3)]
    );

    let receiver = seeded_reassembler(peer, id, 6, &[(0, b"ab"), (4, b"ef")], now);
    assert_eq!(receiver.missing_ranges(peer, id, 3), vec![(2, 2)]);
}

#[test]
fn missing_range_requests_are_due_on_exact_retry_boundary_not_before() {
    use std::time::Duration;
    let now = std::time::Instant::now();
    let peer: IpAddr = "127.0.0.93".parse().unwrap();
    let id = [0x62; 32];
    let mut receiver = seeded_reassembler(peer, id, 10, &[(0, b"abc"), (7, b"hij")], now);
    assert_eq!(
        receiver.missing_ranges_if_due(peer, id, now, Duration::from_secs(3), 0),
        vec![]
    );
    assert_eq!(
        receiver.missing_ranges_if_due(peer, [0; 32], now, Duration::from_secs(3), 4),
        vec![]
    );
    assert_eq!(
        receiver.missing_ranges_if_due(peer, id, now, Duration::from_secs(3), 4),
        vec![(3, 4)]
    );
    assert!(receiver
        .missing_ranges_if_due(
            peer,
            id,
            now + Duration::from_millis(2999),
            Duration::from_secs(3),
            4
        )
        .is_empty());
    assert_eq!(
        receiver.missing_ranges_if_due(
            peer,
            id,
            now + Duration::from_secs(3),
            Duration::from_secs(3),
            4
        ),
        vec![(3, 4)]
    );
    assert!(receiver
        .missing_ranges_if_due(
            peer,
            id,
            now + Duration::from_secs(4),
            Duration::from_secs(3),
            4
        )
        .is_empty());
}

#[test]
fn idle_recovery_budgets_and_retry_interval_survive_missing_terminal() {
    use std::time::Duration;
    let now = std::time::Instant::now();
    let peer: IpAddr = "127.0.0.94".parse().unwrap();
    let id = [0x63; 32];
    let mut receiver = seeded_reassembler(peer, id, 10, &[(0, b"abc")], now);
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(10),
            Duration::from_secs(1),
            Duration::from_secs(3),
            0,
            8
        )
        .is_empty());
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(10),
            Duration::from_secs(1),
            Duration::from_secs(3),
            2,
            0
        )
        .is_empty());
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_millis(999),
            Duration::from_secs(1),
            Duration::from_secs(3),
            2,
            8
        )
        .is_empty());

    let due = receiver.poll_idle_missing(
        now + Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(3),
        2,
        8,
    );
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].peer, peer);
    assert_eq!(due[0].transfer_id, id);
    assert_eq!(due[0].ranges, vec![(3, 7)]);
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(3),
            Duration::from_secs(1),
            Duration::from_secs(3),
            2,
            8
        )
        .is_empty());
    assert_eq!(
        receiver
            .poll_idle_missing(
                now + Duration::from_secs(4),
                Duration::from_secs(1),
                Duration::from_secs(3),
                2,
                8
            )
            .len(),
        1
    );
    assert_eq!(receiver.retained_bytes(), 3);
    assert_eq!(receiver.expire(now + Duration::from_secs(89)), 0);
    assert_eq!(receiver.expire(now + Duration::from_secs(90)), 1);
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(91),
            Duration::from_secs(1),
            Duration::from_secs(3),
            2,
            8
        )
        .is_empty());
}

#[test]
fn idle_poll_prioritizes_stable_peer_order_and_enforces_report_cap() {
    use std::collections::HashMap;
    use std::time::Duration;
    let now = std::time::Instant::now();
    let a: IpAddr = "127.0.0.95".parse().unwrap();
    let b: IpAddr = "127.0.0.96".parse().unwrap();
    let mut receiver = seeded_reassembler(b, [2; 32], 6, &[(0, b"ab")], now);
    let mut second = seeded_reassembler(a, [1; 32], 6, &[(0, b"ab")], now);
    let state = second.peers.remove(&a).unwrap();
    receiver.peers.extend(HashMap::from([(a, state)]));
    receiver.total_retained_bytes += 2;
    let reqs = receiver.poll_idle_missing(
        now + Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(3),
        1,
        1,
    );
    assert_eq!(reqs.len(), 1);
    assert_eq!(
        (reqs[0].peer, reqs[0].transfer_id, reqs[0].ranges.as_slice()),
        (a, [1; 32], &[(2, 4)][..])
    );
    let next = receiver.poll_idle_missing(
        now + Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(3),
        1,
        1,
    );
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].peer, b);
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(3),
            1,
            2
        )
        .is_empty());
}

#[test]
fn malformed_control_frames_reject_exact_boundary_and_overlaps() {
    let id = [0x64; 32];
    let mut bytes = Vec::new();
    write_missing_report(id, &[(0, 2), (2, 3)], &mut bytes).unwrap();
    assert_eq!(
        parse_recovery_control(&bytes, 1),
        Err(RecoveryControlError::TooManyRanges)
    );
    assert_eq!(
        parse_recovery_control(&bytes, 2).unwrap(),
        Some(RecoveryControl::Missing {
            transfer_id: id,
            ranges: vec![(0, 2), (2, 3)],
        })
    );
    assert_eq!(
        parse_recovery_control(&bytes[..MISSING_REPORT_HEADER_LEN - 1], 2),
        Err(RecoveryControlError::Truncated)
    );
    assert_eq!(
        parse_recovery_control(&bytes[..bytes.len() - 1], 2),
        Err(RecoveryControlError::InvalidLength)
    );
    let mut wrong_count = bytes.clone();
    wrong_count[33..35].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        parse_recovery_control(&wrong_count, 2),
        Err(RecoveryControlError::InvalidLength)
    );
    assert_eq!(
        write_missing_report(id, &[(0, 0)], &mut bytes),
        Err(RecoveryControlError::InvalidRange)
    );
    assert_eq!(
        write_missing_report(id, &[(u32::MAX, 2)], &mut bytes),
        Err(RecoveryControlError::InvalidRange)
    );
    assert_eq!(
        write_missing_report(id, &[(0, 4), (3, 2)], &mut bytes),
        Err(RecoveryControlError::InvalidRange)
    );
    write_completion_ack(id, &mut bytes);
    assert_eq!(
        parse_recovery_control(&bytes[..COMPLETION_ACK_LEN - 1], 8),
        Err(RecoveryControlError::Truncated)
    );
    bytes.push(0);
    assert_eq!(
        parse_recovery_control(&bytes, 8),
        Err(RecoveryControlError::InvalidLength)
    );
}

#[test]
fn recovery_control_errors_have_specific_operator_visible_diagnostics() {
    assert_eq!(
        RecoveryControlError::Truncated.to_string(),
        "truncated recovery-control frame"
    );
    assert_eq!(
        RecoveryControlError::InvalidLength.to_string(),
        "invalid recovery-control length"
    );
    assert_eq!(
        RecoveryControlError::TooManyRanges.to_string(),
        "too many missing ranges"
    );
    assert_eq!(
        RecoveryControlError::InvalidRange.to_string(),
        "invalid missing range"
    );
}

#[test]
fn adjacent_ranges_and_fully_present_transfer_are_unambiguous() {
    let id = [0x65; 32];
    let mut bytes = Vec::new();
    write_missing_report(id, &[(3, 2), (5, 3)], &mut bytes).unwrap();
    assert_eq!(
        parse_recovery_control(&bytes, 2).unwrap(),
        Some(RecoveryControl::Missing {
            transfer_id: id,
            ranges: vec![(3, 2), (5, 3)]
        })
    );
    let peer: IpAddr = "127.0.0.97".parse().unwrap();
    let receiver = seeded_reassembler(
        peer,
        id,
        6,
        &[(0, b"abc"), (3, b"def")],
        std::time::Instant::now(),
    );
    assert!(
        receiver.missing_ranges(peer, id, 3).is_empty(),
        "fully covered transfer has no trailing empty gap"
    );
}

#[test]
fn zero_missing_range_budget_never_emits_even_with_leading_gap() {
    use std::time::Duration;
    let now = std::time::Instant::now();
    let peer: IpAddr = "127.0.0.98".parse().unwrap();
    let id = [0x66; 32];
    let mut receiver = seeded_reassembler(peer, id, 10, &[(3, b"abc")], now);
    assert!(receiver
        .poll_idle_missing(
            now + Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(3),
            0,
            5
        )
        .is_empty());
    assert_eq!(receiver.missing_ranges(peer, id, 2), vec![(0, 3), (6, 4)]);
    assert_eq!(
        receiver
            .poll_idle_missing(
                now + Duration::from_secs(2),
                Duration::from_secs(1),
                Duration::from_secs(3),
                2,
                5
            )
            .len(),
        1
    );
}

// The private helper must not produce a trailing range when the budget is zero.
#[test]
fn missing_range_private_helper_honours_zero_limit() {
    let now = std::time::Instant::now();
    let peer: IpAddr = "127.0.0.99".parse().unwrap();
    let id = [0x67; 32];
    let reassembler = seeded_reassembler(peer, id, 8, &[(0, b"abc")], now);
    let transfer = &reassembler.peers.get(&peer).unwrap().transfers[&id];
    assert!(missing_ranges_for_transfer(transfer, 0).is_empty());
    assert_eq!(missing_ranges_for_transfer(transfer, 1), vec![(3, 5)]);
}
