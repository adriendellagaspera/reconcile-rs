// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::{Duration, Instant};

use super::*;
use crate::framing::{
    transfer_id, write_fragment, FrameError, Reassembler, ReassemblyLimits, ReceiveOutcome,
};

fn limits() -> ReassemblyLimits {
    ReassemblyLimits {
        max_logical_message_size: 100,
        max_fragments_per_message: 10,
        max_incomplete_transfers_per_peer: 2,
        max_reassembly_bytes_per_peer: 64,
        max_total_reassembly_bytes: 128,
        reassembly_ttl: Duration::from_secs(10),
    }
}

fn frames() -> (Vec<u8>, Vec<u8>, [u8; 32]) {
    let contents = b"abcdefghij";
    let id = transfer_id(contents);
    let mut first = Vec::new();
    let mut second = Vec::new();
    write_fragment(id, contents.len(), 0, &contents[..4], &mut first).unwrap();
    write_fragment(id, contents.len(), 4, &contents[4..], &mut second).unwrap();
    (first, second, id)
}

#[test]
fn signed_fragments_survive_route_changes_and_preserve_identity_bound_recovery() {
    let sender = boundary(signer_a(), [], 2);
    let receiver = boundary(signer_b(), [sender.local_peer()], 2);
    let mut assembly = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let (first, second, id) = frames();

    let old_route = (4_u16, 2_u8);
    let new_route = (7_u16, 9_u8);
    assert_ne!(old_route, new_route);
    let now = Instant::now();
    let initial = sender.seal(&first).unwrap();
    let (principal, report) = receiver
        .open_frame(&initial, |_| (false, 0), &mut assembly, now)
        .unwrap();
    assert_eq!(principal.peer_id(), sender.local_peer());
    assert!(matches!(report.outcome, Ok(ReceiveOutcome::Pending)));
    assert_eq!(report.retained_bytes, 4);
    assert_eq!(assembly.missing_ranges(principal, id, 4), vec![(4, 6)]);

    let retry = assembly.poll_idle_missing(
        now + Duration::from_millis(200),
        Duration::from_millis(100),
        Duration::from_millis(25),
        4,
        4,
    );
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].peer, principal);
    assert_eq!(retry[0].ranges, vec![(4, 6)]);

    let next = sender.seal(&second).unwrap();
    let (same_principal, report) = receiver
        .open_frame(
            &next,
            |_| (true, 1),
            &mut assembly,
            now + Duration::from_millis(220),
        )
        .unwrap();
    assert_eq!(same_principal, principal);
    match report.outcome {
        Ok(ReceiveOutcome::Complete(plain)) => assert_eq!(plain.as_bytes(), b"abcdefghij"),
        other => panic!("unexpected completion outcome: {other:?}"),
    }
    assert_eq!(assembly.retained_bytes(), 0);
    assert_eq!(receiver.replay_senders(), 1);
    assert_eq!(
        receiver
            .open_frame(&initial, |_| (true, 1), &mut assembly, now)
            .err(),
        Some(SignedDatagramError::Replay)
    );
}

#[test]
fn distinct_verified_signers_do_not_share_fragment_transfers_or_quotas() {
    let alice = boundary(signer_a(), [], 2);
    let bob = boundary(signer_b(), [], 2);
    let receiving = SignedDatagramBoundary::new(
        key(),
        PeerSigner::from_seed([0x33; 32]),
        PeerAllowlist::new([alice.local_peer(), bob.local_peer()]),
        FRESHNESS_WINDOW_DEFAULT,
        2,
        2,
        MTU,
    )
    .unwrap();
    let (first, second, id) = frames();
    let mut assembly = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let now = Instant::now();

    let a1 = alice.seal(&first).unwrap();
    let b1 = bob.seal(&first).unwrap();
    let (a, a_report) = receiving
        .open_frame(&a1, |_| (false, 0), &mut assembly, now)
        .unwrap();
    let (b, b_report) = receiving
        .open_frame(&b1, |_| (false, 1), &mut assembly, now)
        .unwrap();
    assert_ne!(a, b);
    assert!(matches!(a_report.outcome, Ok(ReceiveOutcome::Pending)));
    assert!(matches!(b_report.outcome, Ok(ReceiveOutcome::Pending)));
    assert_eq!(assembly.retained_bytes(), 8);
    assert_eq!(assembly.missing_ranges(a, id, 1), vec![(4, 6)]);
    assert_eq!(assembly.missing_ranges(b, id, 1), vec![(4, 6)]);

    let a2 = alice.seal(&second).unwrap();
    let (_, completed) = receiving
        .open_frame(&a2, |_| (true, 2), &mut assembly, now)
        .unwrap();
    assert!(matches!(completed.outcome, Ok(ReceiveOutcome::Complete(_))));
    assert_eq!(assembly.retained_bytes(), 4);
    assert_eq!(assembly.missing_ranges(b, id, 1), vec![(4, 6)]);

    let b2 = bob.seal(&second).unwrap();
    let (_, completed) = receiving
        .open_frame(&b2, |_| (true, 2), &mut assembly, now)
        .unwrap();
    assert!(matches!(completed.outcome, Ok(ReceiveOutcome::Complete(_))));
    assert_eq!(assembly.retained_bytes(), 0);
    assert_eq!(receiving.replay_senders(), 2);
}

#[test]
fn untrusted_signed_packet_never_allocates_fragment_state() {
    let unauthorized = boundary(signer_a(), [], 1);
    let receiving = boundary(signer_b(), [], 1);
    let (first, _, _) = frames();
    let signed = unauthorized.seal(&first).unwrap();
    let mut assembly = Reassembler::<VerifiedPeerId>::for_identity(limits());
    assert_eq!(
        receiving
            .open_frame(
                &signed,
                |_| panic!("unauthorized signature must not consult member state"),
                &mut assembly,
                Instant::now(),
            )
            .err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::InvalidProof))
    );
    assert_eq!(receiving.replay_senders(), 0);
    assert_eq!(assembly.retained_bytes(), 0);
}

#[test]
fn per_principal_fragment_limits_are_not_bypassed_by_a_route_move() {
    let sender = boundary(signer_a(), [], 1);
    let receiver = boundary(signer_b(), [sender.local_peer()], 1);
    let mut small = limits();
    small.max_reassembly_bytes_per_peer = 4;
    let mut assembly = Reassembler::<VerifiedPeerId>::for_identity(small);
    let (first, second, _) = frames();
    let now = Instant::now();

    let first_packet = sender.seal(&first).unwrap();
    let (_, first_report) = receiver
        .open_frame(&first_packet, |_| (false, 0), &mut assembly, now)
        .unwrap();
    assert!(matches!(first_report.outcome, Ok(ReceiveOutcome::Pending)));

    let next_packet = sender.seal(&second).unwrap();
    let (_, second_report) = receiver
        .open_frame(&next_packet, |_| (true, 1), &mut assembly, now)
        .unwrap();
    assert!(matches!(
        second_report.outcome,
        Err(FrameError::ReassemblyCapacity)
    ));
    assert_eq!(assembly.retained_bytes(), 4);
}
