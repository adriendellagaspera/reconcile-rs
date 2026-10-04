// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use crate::auth::{Authenticator, Payload, Verified};
use crate::replay::{ReplayFilter, SenderCounter, FRESHNESS_WINDOW_DEFAULT};

fn verified<'a>(auth: &Authenticator, wire: &'a [u8]) -> Payload<'a, Verified> {
    auth.open(wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(
            &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
            "127.0.0.1".parse().unwrap(),
        )
        .unwrap()
}

fn limits() -> ReassemblyLimits {
    ReassemblyLimits {
        max_logical_message_size: 1024,
        max_fragments_per_message: 16,
        max_incomplete_transfers_per_peer: 2,
        max_reassembly_bytes_per_peer: 1024,
        max_total_reassembly_bytes: 2048,
        reassembly_ttl: Duration::from_secs(10),
    }
}

#[derive(Debug, Eq, PartialEq)]
enum TestOutcome {
    Complete(Vec<u8>),
    Pending,
    Duplicate,
    Rejected(FrameError),
}

#[derive(Debug)]
struct TestReport {
    outcome: TestOutcome,
    evicted_transfers: usize,
    retained_bytes: usize,
}

struct FragmentSpec<'a> {
    id: [u8; 32],
    total_len: usize,
    offset: usize,
    bytes: &'a [u8],
}

fn feed_fragment(
    auth: &Authenticator,
    counter: &SenderCounter,
    receiver: &mut Reassembler,
    peer: IpAddr,
    fragment: FragmentSpec<'_>,
    now: Instant,
) -> TestReport {
    let mut frame = Vec::new();
    write_fragment(
        fragment.id,
        fragment.total_len,
        fragment.offset,
        fragment.bytes,
        &mut frame,
    )
    .unwrap();
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    let report = receiver.accept(peer, verified(auth, &wire), now);
    let outcome = match report.outcome {
        Ok(ReceiveOutcome::Complete(payload)) => TestOutcome::Complete(payload.as_bytes().to_vec()),
        Ok(ReceiveOutcome::Pending) => TestOutcome::Pending,
        Ok(ReceiveOutcome::Duplicate) => TestOutcome::Duplicate,
        Err(error) => TestOutcome::Rejected(error),
    };
    TestReport {
        outcome,
        evicted_transfers: report.evicted_transfers,
        retained_bytes: report.retained_bytes,
    }
}

#[test]
fn complete_frame_round_trips_without_reassembly_allocation() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let mut frame = Vec::new();
    write_complete(b"hello", &mut frame);
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    let mut receiver = Reassembler::new(limits());
    let report = receiver.accept(
        "127.0.0.1".parse().unwrap(),
        verified(&auth, &wire),
        Instant::now(),
    );
    match report.outcome.unwrap() {
        ReceiveOutcome::Complete(payload) => assert_eq!(payload.as_bytes(), b"hello"),
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn complete_frame_respects_logical_message_limit() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let mut frame = Vec::new();
    write_complete(b"too-large", &mut frame);
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_logical_message_size: 4,
        ..limits()
    });

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
fn out_of_order_fragments_complete_and_duplicate_reuses_progress() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let message = b"abcdefghij";
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
                offset: 5,
                bytes: b"fghij",
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
                total_len: message.len(),
                offset: 5,
                bytes: b"fghij",
            },
            now,
        )
        .outcome,
        TestOutcome::Duplicate
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
                offset: 0,
                bytes: b"abcde",
            },
            now,
        )
        .outcome,
        TestOutcome::Complete(message.to_vec())
    );
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn ttl_and_peer_transfer_cap_evict_deterministically() {
    let peer = "127.0.0.1".parse().unwrap();
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_incomplete_transfers_per_peer: 1,
        reassembly_ttl: Duration::from_millis(10),
        ..limits()
    });
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let start = Instant::now();

    for (index, message) in [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .enumerate()
    {
        let report = feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(message),
                total_len: message.len(),
                offset: 0,
                bytes: &message[..1],
            },
            start + Duration::from_millis(index as u64),
        );
        assert_eq!(report.evicted_transfers, usize::from(index == 1));
    }

    assert_eq!(receiver.expire(start + Duration::from_millis(20)), 1);
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn retransmission_reuses_retained_fragments_after_a_gap() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let message = b"abcdefghijkl";
    let id = transfer_id(message);
    let now = Instant::now();
    let mut receiver = Reassembler::new(limits());

    for (offset, bytes) in [(0, b"abcd".as_slice()), (8, b"ijkl".as_slice())] {
        assert_eq!(
            feed_fragment(
                &auth,
                &counter,
                &mut receiver,
                peer,
                FragmentSpec {
                    id,
                    total_len: message.len(),
                    offset,
                    bytes,
                },
                now,
            )
            .outcome,
            TestOutcome::Pending
        );
    }
    assert_eq!(receiver.retained_bytes(), 8);

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
            now + Duration::from_millis(1),
        )
        .outcome,
        TestOutcome::Duplicate
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
                offset: 4,
                bytes: b"efgh",
            },
            now + Duration::from_millis(1),
        )
        .outcome,
        TestOutcome::Complete(message.to_vec())
    );
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn two_interleaved_transfers_from_one_peer_complete_independently() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();
    let a = b"abcdefgh";
    let b = b"12345678";
    let now = Instant::now();
    let mut receiver = Reassembler::new(limits());

    for (message, bytes) in [
        (a.as_slice(), b"abcd".as_slice()),
        (b.as_slice(), b"1234".as_slice()),
    ] {
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
                    bytes,
                },
                now,
            )
            .outcome,
            TestOutcome::Pending
        );
    }

    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(a),
                total_len: a.len(),
                offset: 4,
                bytes: b"efgh",
            },
            now,
        )
        .outcome,
        TestOutcome::Complete(a.to_vec())
    );
    assert_eq!(
        feed_fragment(
            &auth,
            &counter,
            &mut receiver,
            peer,
            FragmentSpec {
                id: transfer_id(b),
                total_len: b.len(),
                offset: 4,
                bytes: b"5678",
            },
            now,
        )
        .outcome,
        TestOutcome::Complete(b.to_vec())
    );
    assert_eq!(receiver.retained_bytes(), 0);
}

#[test]
fn exact_frame_boundaries_and_capacity_math_are_inclusive() {
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let peer = "127.0.0.1".parse().unwrap();

    let mut frame = Vec::new();
    write_complete(b"four", &mut frame);
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    let mut receiver = Reassembler::new(ReassemblyLimits {
        max_logical_message_size: 4,
        ..limits()
    });
    match receiver
        .accept(peer, verified(&auth, &wire), Instant::now())
        .outcome
        .unwrap()
    {
        ReceiveOutcome::Complete(payload) => assert_eq!(payload.as_bytes(), b"four"),
        other => panic!("unexpected exact-limit outcome: {other:?}"),
    }

    let exact_header = vec![FRAGMENT_TAG; FRAGMENT_HEADER_LEN];
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &exact_header);
    assert_eq!(
        receiver
            .accept(peer, verified(&auth, &wire), Instant::now())
            .outcome
            .unwrap_err(),
        FrameError::EmptyFragment
    );

    assert_eq!(complete_payload_capacity(1_200, 49), Some(1_150));
    assert_eq!(complete_payload_capacity(50, 49), Some(0));
    assert_eq!(complete_payload_capacity(49, 49), None);
    assert_eq!(fragment_payload_capacity(1_200, 49), Some(1_110));
    assert_eq!(fragment_payload_capacity(90, 49), Some(0));
    assert_eq!(fragment_payload_capacity(89, 49), None);
}

#[test]
fn frame_error_categories_and_display_are_stable() {
    for error in [
        FrameError::LogicalMessageTooLarge,
        FrameError::TooManyFragments,
        FrameError::ReassemblyCapacity,
    ] {
        assert!(
            error.is_limit(),
            "{error:?} must remain a resource-limit rejection"
        );
    }
    for error in [
        FrameError::Empty,
        FrameError::UnknownTag(7),
        FrameError::TruncatedFragmentHeader,
        FrameError::EmptyFragment,
        FrameError::InvalidFragmentBounds,
        FrameError::InconsistentMetadata,
        FrameError::OverlappingFragment,
        FrameError::HashMismatch,
    ] {
        assert!(
            !error.is_limit(),
            "{error:?} must not be classified as a limit"
        );
    }
    assert_eq!(FrameError::Empty.to_string(), "empty framing payload");
    assert_eq!(
        FrameError::UnknownTag(7).to_string(),
        "unknown framing tag 7"
    );
    assert_eq!(
        FrameError::ReassemblyCapacity.to_string(),
        "reassembly byte capacity exhausted"
    );
}

mod bounds;
