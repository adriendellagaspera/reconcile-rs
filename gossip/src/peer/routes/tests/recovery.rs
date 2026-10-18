// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

use crate::framing::{
    write_completion_ack, write_missing_report, RecoveryControl, COMPLETION_ACK_TAG,
    MISSING_REPORT_TAG,
};

#[test]
fn signed_recovery_controls_require_a_confirmed_principal_and_source_route() {
    let (alice, bob) = endpoints();
    let now = Instant::now();
    let bob_addr: Slot = (6, 1);
    let original: Slot = (2, 2);
    let moved: Slot = (2, 9);
    let mut routes = SignedPeerRoutes::new(2, Duration::from_secs(1));
    let mut alice_routes = SignedPeerRoutes::new(2, Duration::from_secs(1));
    let mut frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let mut alice_frames = Reassembler::<VerifiedPeerId>::for_identity(limits());

    let initial = alice.seal(&frame(b"hello")).unwrap();
    let challenge = match routes
        .receive(&bob, &initial, original, |_| (false, 0), &mut frames, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("first signed frame must prove returnability"),
    };
    let reply = match alice_routes
        .receive(
            &alice,
            &challenge.bytes,
            bob_addr,
            |_| (false, 0),
            &mut alice_frames,
            now,
        )
        .unwrap()
    {
        SignedRouteReceive::Reply(reply) => reply,
        _ => panic!("a valid signed challenge must get a signed answer"),
    };
    assert!(matches!(
        routes
            .receive(
                &bob,
                &reply.bytes,
                original,
                |_| (false, 0),
                &mut frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Confirmed(_)
    ));
    assert_eq!(routes.confirmed_peers().len(), 1);
    assert_eq!(routes.confirmed_peers()[0].peer_id(), alice.local_peer());

    let transfer_id = [0x2a; 32];
    let mut missing = Vec::new();
    write_missing_report(transfer_id, &[(4, 8)], &mut missing).unwrap();
    let signed = alice.seal(&missing).unwrap();
    match routes
        .receive(&bob, &signed, original, |_| (false, 0), &mut frames, now)
        .unwrap()
    {
        SignedRouteReceive::Recovery {
            peer,
            control:
                RecoveryControl::Missing {
                    transfer_id: id,
                    ranges,
                },
        } => {
            assert_eq!(peer.peer_id(), alice.local_peer());
            assert_eq!(id, transfer_id);
            assert_eq!(ranges, vec![(4, 8)]);
        }
        _ => panic!("confirmed signed recovery report must reach the authenticated dispatcher"),
    }

    let moved_control = alice.seal(&missing).unwrap();
    assert!(matches!(
        routes
            .receive(
                &bob,
                &moved_control,
                moved,
                |_| (false, 0),
                &mut frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: Some(_),
            ..
        }
    ));
    assert_eq!(routes.route(alice.local_peer()), Some(&original));
    assert_eq!(routes.confirmed_peers().len(), 1);

    let malformed = alice.seal(&[MISSING_REPORT_TAG, 1]).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &malformed, original, |_| (false, 0), &mut frames, now)
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
    let mut complete = Vec::new();
    write_completion_ack(transfer_id, &mut complete);
    assert_eq!(complete[0], COMPLETION_ACK_TAG);
    let signed = alice.seal(&complete).unwrap();
    match routes
        .receive(&bob, &signed, original, |_| (false, 0), &mut frames, now)
        .unwrap()
    {
        SignedRouteReceive::Recovery {
            peer,
            control: RecoveryControl::CompleteAck { transfer_id: id },
        } => {
            assert_eq!(peer.peer_id(), alice.local_peer());
            assert_eq!(id, transfer_id);
        }
        _ => panic!("confirmed signed completion must retain its principal"),
    }
    assert_eq!(frames.retained_bytes(), 0);
    routes.decommission(alice.local_peer());
    assert!(routes.confirmed_peers().is_empty());
}
