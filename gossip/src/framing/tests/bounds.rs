// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

#[test]
fn peer_and_global_byte_caps_evict_oldest_incomplete_transfer() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peers = [
        "127.0.0.1".parse().unwrap(),
        "127.0.0.2".parse().unwrap(),
        "127.0.0.3".parse().unwrap(),
    ];
    let now = Instant::now();
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 4,
        max_total_reassembly_bytes: 6,
        max_incomplete_transfers_per_peer: 4,
        ..limits()
    });

    for (index, peer) in peers[..2].iter().copied().enumerate() {
        let message = if index == 0 {
            b"aaaaaa".as_slice()
        } else {
            b"bbbbbb".as_slice()
        };
        let report = feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(message),
                total_len: message.len(),
                offset: 0,
                bytes: &message[..3],
            },
            now + Duration::from_millis(index as u64),
        );
        assert_eq!(report.evicted_transfers, 0);
    }

    let report = feed_fragment(
        &auth,
        &counter,
        &mut receiver,
        peers[2],
        FragmentSpec {
            id: transfer_id(b"cccccc"),
            total_len: 6,
            offset: 0,
            bytes: b"ccc",
        },
        now + Duration::from_millis(2),
    );
    assert_eq!(report.evicted_transfers, 1);
    assert_eq!(report.retained_bytes, 6);

    let report = feed_fragment(
        &auth,
        &counter,
        &mut receiver,
        peers[2],
        FragmentSpec {
            id: transfer_id(b"dddddd"),
            total_len: 6,
            offset: 0,
            bytes: b"ddd",
        },
        now + Duration::from_millis(3),
    );
    assert_eq!(report.evicted_transfers, 1);
    assert_eq!(report.retained_bytes, 6);
}

#[test]
fn inconsistent_and_overlapping_fragment_metadata_is_rejected() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let message = b"abcdefgh";
    let id = transfer_id(message);
    let now = Instant::now();
    let mut receiver = Reassembler::new(limits());

    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id,
                total_len: message.len(),
                offset: 0,
                bytes: b"abcd",
            },
            now,
        )
        .outcome,
        TestOutcome::Pending
    );
    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id,
                total_len: message.len() + 1,
                offset: 4,
                bytes: b"efgh",
            },
            now,
        )
        .outcome,
        TestOutcome::Rejected(FrameError::InconsistentMetadata)
    );
    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id,
                total_len: message.len(),
                offset: 2,
                bytes: b"cdef",
            },
            now,
        )
        .outcome,
        TestOutcome::Rejected(FrameError::OverlappingFragment)
    );
    assert_eq!(receiver.retained_bytes(), 4);
}

#[test]
fn unauthenticated_and_replayed_fragments_cannot_extend_reassembly() {
    let auth = Authenticator::new(Some(crate::auth::ClusterKey::new([7; 32])), false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let message = b"abcdefgh";
    let id = transfer_id(message);
    let now = Instant::now();
    let filter = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true);
    let mut receiver = Reassembler::new(limits());

    let mut frame = Vec::new();
    write_fragment(id, message.len(), 0, b"abcd", &mut frame).unwrap();
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);

    let mut forged = wire.clone();
    forged[0] ^= 0x80;
    assert!(auth.open(&forged).is_none());
    assert_eq!(receiver.retained_bytes(), 0);

    let admitted = auth
        .open(&wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&filter, peer)
        .unwrap();
    assert!(matches!(
        receiver.accept(peer, admitted, now).outcome.unwrap(),
        ReceiveOutcome::Pending
    ));
    assert_eq!(receiver.retained_bytes(), 4);

    assert!(auth
        .open(&wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&filter, peer)
        .is_none());
    assert_eq!(receiver.retained_bytes(), 4);

    write_fragment(id, message.len(), 4, b"efgh", &mut frame).unwrap();
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    let admitted = auth
        .open(&wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&filter, peer)
        .unwrap();
    match receiver.accept(peer, admitted, now).outcome.unwrap() {
        ReceiveOutcome::Complete(payload) => assert_eq!(payload.as_bytes(), message),
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn malformed_and_over_limit_fragment_metadata_is_rejected() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let mut receiver = Reassembler::new(limits());

    let truncated = vec![FRAGMENT_TAG; FRAGMENT_HEADER_LEN - 1];
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &truncated);
    assert_eq!(
        receiver
            .accept(peer, verified(&auth, &wire), Instant::now())
            .outcome
            .unwrap_err(),
        FrameError::TruncatedFragmentHeader
    );

    let mut frame = Vec::new();
    let id = transfer_id(&vec![0u8; 2048]);
    write_fragment(id, 2048, 0, b"x", &mut frame).unwrap();
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    assert_eq!(
        receiver
            .accept(peer, verified(&auth, &wire), Instant::now())
            .outcome
            .unwrap_err(),
        FrameError::LogicalMessageTooLarge
    );
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn fragment_length_and_range_boundaries_are_independent() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let now = Instant::now();

    let message = b"four";
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_logical_message_size: message.len(),
        ..limits()
    });
    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(message),
                total_len: message.len(),
                offset: 0,
                bytes: message,
            },
            now,
        )
        .outcome,
        TestOutcome::Complete(message.to_vec())
    );

    for (total_len, offset, bytes) in [
        (0usize, 0usize, b"x".as_slice()),
        (4usize, 4usize, b"x".as_slice()),
        (4usize, 3usize, b"xy".as_slice()),
    ] {
        let payload = vec![b'z'; total_len.max(offset + bytes.len())];
        let report = feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(&payload),
                total_len,
                offset,
                bytes,
            },
            now,
        );
        assert_eq!(
            report.outcome,
            TestOutcome::Rejected(FrameError::InvalidFragmentBounds),
            "total_len={total_len}, offset={offset}, fragment_len={}",
            bytes.len()
        );
    }
}

#[test]
fn exact_byte_caps_are_allowed_and_only_excess_evicts() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let now = Instant::now();

    let mut peer_bounded = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 4,
        max_total_reassembly_bytes: 100,
        max_incomplete_transfers_per_peer: 4,
        ..limits()
    });
    let first = feed_fragment(
        &auth,
        &counter,
        &mut peer_bounded,
        peer,
        FragmentSpec {
            id: transfer_id(b"aaaaaaaa"),
            total_len: 8,
            offset: 0,
            bytes: b"aaaa",
        },
        now,
    );
    assert_eq!(first.outcome, TestOutcome::Pending);
    assert_eq!(first.evicted_transfers, 0);
    assert_eq!(first.retained_bytes, 4);

    let second = feed_fragment(
        &auth,
        &counter,
        &mut peer_bounded,
        peer,
        FragmentSpec {
            id: transfer_id(b"bbbbbbbb"),
            total_len: 8,
            offset: 0,
            bytes: b"bb",
        },
        now + Duration::from_millis(1),
    );
    assert_eq!(second.outcome, TestOutcome::Pending);
    assert_eq!(second.evicted_transfers, 1);
    assert_eq!(second.retained_bytes, 2);

    let peers = [
        "127.0.0.2".parse().unwrap(),
        "127.0.0.3".parse().unwrap(),
        "127.0.0.4".parse().unwrap(),
    ];
    let mut global_bounded = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 100,
        max_total_reassembly_bytes: 4,
        max_incomplete_transfers_per_peer: 4,
        ..limits()
    });
    for (index, peer) in peers[..2].iter().copied().enumerate() {
        let message = if index == 0 {
            b"cccc".as_slice()
        } else {
            b"dddd".as_slice()
        };
        let report = feed_fragment(
            &auth,
            &counter,
            &mut global_bounded,
            peer,
            FragmentSpec {
                id: transfer_id(message),
                total_len: 4,
                offset: 0,
                bytes: &message[..2],
            },
            now + Duration::from_millis(index as u64),
        );
        assert_eq!(report.outcome, TestOutcome::Pending);
        assert_eq!(report.evicted_transfers, 0);
    }
    assert_eq!(global_bounded.retained_bytes(), 4);

    let report = feed_fragment(
        &auth,
        &counter,
        &mut global_bounded,
        peers[2],
        FragmentSpec {
            id: transfer_id(b"eeee"),
            total_len: 4,
            offset: 0,
            bytes: b"e",
        },
        now + Duration::from_millis(2),
    );
    assert_eq!(report.outcome, TestOutcome::Pending);
    assert_eq!(report.evicted_transfers, 1);
    assert_eq!(report.retained_bytes, 3);
}

#[test]
fn incoming_fragment_caps_are_independent_and_inclusive() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.9".parse().unwrap();
    let now = Instant::now();

    let mut exact_global = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 100,
        max_total_reassembly_bytes: 4,
        ..limits()
    });
    let report = feed_fragment(
        &auth,
        &counter,
        &mut exact_global,
        peer,
        FragmentSpec {
            id: transfer_id(b"abcdefgh"),
            total_len: 8,
            offset: 0,
            bytes: b"abcd",
        },
        now,
    );
    assert_eq!(report.outcome, TestOutcome::Pending);
    assert_eq!(report.retained_bytes, 4);

    let mut peer_only = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 3,
        max_total_reassembly_bytes: 100,
        ..limits()
    });
    let report = feed_fragment(
        &auth,
        &counter,
        &mut peer_only,
        peer,
        FragmentSpec {
            id: transfer_id(b"ijklmnop"),
            total_len: 8,
            offset: 0,
            bytes: b"ijkl",
        },
        now,
    );
    assert_eq!(
        report.outcome,
        TestOutcome::Rejected(FrameError::ReassemblyCapacity)
    );
    assert_eq!(report.retained_bytes, 0);

    let mut global_only = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 100,
        max_total_reassembly_bytes: 3,
        ..limits()
    });
    let report = feed_fragment(
        &auth,
        &counter,
        &mut global_only,
        peer,
        FragmentSpec {
            id: transfer_id(b"qrstuvwx"),
            total_len: 8,
            offset: 0,
            bytes: b"qrst",
        },
        now,
    );
    assert_eq!(
        report.outcome,
        TestOutcome::Rejected(FrameError::ReassemblyCapacity)
    );
    assert_eq!(report.retained_bytes, 0);
}

#[test]
fn intrinsically_oversized_new_fragment_does_not_evict_existing_transfer() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.10".parse().unwrap();
    let now = Instant::now();
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_reassembly_bytes_per_peer: 3,
        max_total_reassembly_bytes: 100,
        max_incomplete_transfers_per_peer: 1,
        ..limits()
    });

    let existing = feed_fragment(
        &auth,
        &counter,
        &mut receiver,
        peer,
        FragmentSpec {
            id: transfer_id(b"existing"),
            total_len: 8,
            offset: 0,
            bytes: b"e",
        },
        now,
    );
    assert_eq!(existing.outcome, TestOutcome::Pending);
    assert_eq!(existing.retained_bytes, 1);

    let rejected = feed_fragment(
        &auth,
        &counter,
        &mut receiver,
        peer,
        FragmentSpec {
            id: transfer_id(b"incoming"),
            total_len: 8,
            offset: 0,
            bytes: b"inco",
        },
        now + Duration::from_millis(1),
    );
    assert_eq!(
        rejected.outcome,
        TestOutcome::Rejected(FrameError::ReassemblyCapacity)
    );
    assert_eq!(
        rejected.evicted_transfers, 0,
        "an intrinsically oversized fragment must be rejected before transfer-slot eviction"
    );
    assert_eq!(
        rejected.retained_bytes, 1,
        "the previously retained transfer must remain intact"
    );
}
