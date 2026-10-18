// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use crate::auth::{Authenticator, ClusterKey};
use crate::framing::{write_complete, ReassemblyLimits, ReceiveOutcome};
use crate::peer::identity::{PeerAllowlist, PeerSigner};
use crate::replay::FRESHNESS_WINDOW_DEFAULT;

const MTU: usize = 1200;
type Slot = (u16, u8);

fn boundary(seed: u8, accepted: &[PeerId]) -> SignedDatagramBoundary {
    SignedDatagramBoundary::new(
        Authenticator::new(Some(ClusterKey::new([0x41; 32])), false).unwrap(),
        PeerSigner::from_seed([seed; 32]),
        PeerAllowlist::new(accepted.iter().copied()),
        FRESHNESS_WINDOW_DEFAULT,
        4,
        4,
        MTU,
    )
    .unwrap()
}

fn limits() -> ReassemblyLimits {
    ReassemblyLimits {
        max_logical_message_size: 512,
        max_fragments_per_message: 16,
        max_incomplete_transfers_per_peer: 4,
        max_reassembly_bytes_per_peer: 512,
        max_total_reassembly_bytes: 1024,
        reassembly_ttl: Duration::from_secs(10),
    }
}

fn frame(text: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_complete(text, &mut out);
    out
}

fn endpoints() -> (SignedDatagramBoundary, SignedDatagramBoundary) {
    let a_id = PeerSigner::from_seed([0x11; 32]).peer_id();
    let b_id = PeerSigner::from_seed([0x22; 32]).peer_id();
    (boundary(0x11, &[b_id]), boundary(0x22, &[a_id]))
}

#[test]
fn a_signed_frame_cannot_dispatch_or_redirect_until_return_route_is_proven() {
    let (alice, bob) = endpoints();
    let old: Slot = (2, 1);
    let relocated: Slot = (2, 8);
    let bob_addr: Slot = (5, 3);
    let mut routes = SignedPeerRoutes::new(2, Duration::from_secs(3));
    let mut bob_frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let mut alice_routes = SignedPeerRoutes::new(2, Duration::from_secs(3));
    let mut alice_frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let now = Instant::now();

    assert!(routes.is_empty());
    assert_eq!(
        routes
            .seal_to(&bob, alice.local_peer(), &frame(b"blocked"))
            .err(),
        Some(SignedRouteError::NoConfirmedRoute)
    );
    let first = alice.seal(&frame(b"not-dispatched")).unwrap();
    let challenge = match routes
        .receive(&bob, &first, old, |_| (false, 0), &mut bob_frames, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { peer, challenge } => {
            assert_eq!(peer.peer_id(), alice.local_peer());
            challenge.unwrap()
        }
        _ => panic!("unknown route must be challenged before dispatch"),
    };
    assert_eq!(challenge.destination, old);
    assert_eq!(bob_frames.retained_bytes(), 0);
    assert_eq!(routes.len(), 0);
    assert!(routes.route(alice.local_peer()).is_none());

    // An authenticated packet from a different source cannot consume or replace the pending
    // route. It needs its own challenge/returnability round trip.
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
        _ => panic!("addressed signer must return a signed route challenge response"),
    };
    assert_eq!(reply.destination, bob_addr);
    assert!(matches!(
        routes
            .receive(
                &bob,
                &reply.bytes,
                relocated,
                |_| (false, 0),
                &mut bob_frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
    assert!(routes.route(alice.local_peer()).is_none());

    // The same nonce signed afresh is accepted only from the challenge's exact source route.
    // The route reply already consumed replay admission at the wrong endpoint; resend a
    // *newly signed* reply under the same nonce, extracted from the challenge by its recipient.
    let probe = routes.pending.get(&alice.local_peer()).unwrap();
    let fresh = alice
        .seal(&encode_control(RESPONSE, bob.local_peer(), probe.nonce))
        .unwrap();
    let confirmed = routes
        .receive(&bob, &fresh, old, |_| (false, 0), &mut bob_frames, now)
        .unwrap();
    assert!(matches!(confirmed, SignedRouteReceive::Confirmed(_)));
    assert_eq!(routes.route(alice.local_peer()), Some(&old));
    assert_eq!(routes.len(), 1);
    assert!(!routes.is_empty());
    let outgoing = routes
        .seal_to(&bob, alice.local_peer(), &frame(b"return"))
        .unwrap();
    assert_eq!(outgoing.destination, old);

    let payload = alice.seal(&frame(b"authorized")).unwrap();
    let admitted = routes
        .receive(&bob, &payload, old, |_| (true, 1), &mut bob_frames, now)
        .unwrap();
    match admitted {
        SignedRouteReceive::Framed { peer, report } => {
            assert_eq!(peer.peer_id(), alice.local_peer());
            match report.outcome {
                Ok(ReceiveOutcome::Complete(data)) => assert_eq!(data.as_bytes(), b"authorized"),
                other => panic!("unexpected framed result: {other:?}"),
            }
        }
        _ => panic!("confirmed source should deliver a complete frame"),
    }

    let moved = alice.seal(&frame(b"new-route")).unwrap();
    let challenge = match routes
        .receive(&bob, &moved, relocated, |_| (true, 1), &mut bob_frames, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("route change requires a new returnability check"),
    };
    assert_eq!(routes.route(alice.local_peer()), Some(&old));
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
        _ => panic!("expected signed reply"),
    };
    assert!(matches!(
        routes
            .receive(
                &bob,
                &reply.bytes,
                relocated,
                |_| (true, 1),
                &mut bob_frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Confirmed(_)
    ));
    assert_eq!(routes.route(alice.local_peer()), Some(&relocated));
    let sent = routes
        .seal_to(&bob, alice.local_peer(), &frame(b"after-move"))
        .unwrap();
    assert_eq!(sent.destination, relocated);
    routes.decommission(alice.local_peer());
    assert!(routes.is_empty());
    assert_eq!(
        routes
            .seal_to(&bob, alice.local_peer(), &frame(b"denied"))
            .err(),
        Some(SignedRouteError::NoConfirmedRoute)
    );
}

#[test]
fn malformed_or_expired_route_proof_cannot_promote_an_address() {
    let (alice, bob) = endpoints();
    let now = Instant::now();
    let source: Slot = (10, 4);
    let mut routes = SignedPeerRoutes::new(1, Duration::from_millis(100));
    let mut receiver = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let first = alice.seal(&frame(b"hello")).unwrap();
    let challenge = match routes
        .receive(&bob, &first, source, |_| (false, 0), &mut receiver, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("expected probe"),
    };
    let mut incorrect_nonce = routes.pending.get(&alice.local_peer()).unwrap().nonce;
    incorrect_nonce[0] ^= 1;
    assert!(matches!(
        routes
            .receive(
                &bob,
                &alice
                    .seal(&encode_control(RESPONSE, bob.local_peer(), incorrect_nonce))
                    .unwrap(),
                source,
                |_| (false, 0),
                &mut receiver,
                now
            )
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
    let alice_reply = alice
        .seal(&encode_control(
            RESPONSE,
            bob.local_peer(),
            routes.pending.get(&alice.local_peer()).unwrap().nonce,
        ))
        .unwrap();
    assert!(matches!(
        routes
            .receive(
                &bob,
                &alice_reply,
                source,
                |_| (false, 0),
                &mut receiver,
                now + Duration::from_millis(101)
            )
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
    assert!(routes.route(alice.local_peer()).is_none());
    assert!(!challenge.bytes.is_empty());

    let malformed = alice.seal(&[RESPONSE, 0x01]).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &malformed, source, |_| (false, 0), &mut receiver, now)
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
}

#[test]
fn unallowlisted_sender_does_not_allocate_route_or_reassembly_state() {
    let (alice, _) = endpoints();
    let stranger = boundary(0x44, &[]);
    let bob = boundary(0x22, &[alice.local_peer()]);
    let mut routes = SignedPeerRoutes::<Slot>::new(1, Duration::from_secs(1));
    let mut frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let packet = stranger.seal(&frame(b"attempt")).unwrap();
    assert!(matches!(
        routes.receive(
            &bob,
            &packet,
            (9, 8),
            |_| panic!("unallowlisted identity may not query the causal membership"),
            &mut frames,
            Instant::now(),
        ),
        Err(SignedRouteError::Signed(_))
    ));
    assert_eq!(routes.len(), 0);
    assert!(routes.pending.is_empty());
    assert_eq!(frames.retained_bytes(), 0);
}

#[tokio::test]
async fn signed_routes_forward_over_non_ip_fabric_after_probe_confirmation() {
    use crate::substrate::DatagramFabric;

    let (alice, bob) = endpoints();
    let medium = DatagramFabric::<Slot>::new(MTU);
    let a = (1, 2);
    let b = (6, 7);
    let alice_port = medium.bind(a);
    let bob_port = medium.bind(b);
    let mut alice_routes = SignedPeerRoutes::new(2, Duration::from_secs(5));
    let mut bob_routes = SignedPeerRoutes::new(2, Duration::from_secs(5));
    let mut alice_frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let mut bob_frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let mut buffer = vec![0; MTU];
    let now = Instant::now();

    alice_port
        .send_to(&alice.seal(&frame(b"initial")).unwrap(), &b)
        .unwrap();
    let (n, observed) = bob_port.recv_from(&mut buffer).await.unwrap();
    assert_eq!(observed, a);
    let challenge = match bob_routes
        .receive(
            &bob,
            &buffer[..n],
            observed,
            |_| (false, 0),
            &mut bob_frames,
            now,
        )
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("must probe source first"),
    };
    bob_port
        .send_to(&challenge.bytes, &challenge.destination)
        .unwrap();
    let (n, observed) = alice_port.recv_from(&mut buffer).await.unwrap();
    assert_eq!(observed, b);
    let reply = match alice_routes
        .receive(
            &alice,
            &buffer[..n],
            observed,
            |_| (false, 0),
            &mut alice_frames,
            now,
        )
        .unwrap()
    {
        SignedRouteReceive::Reply(reply) => reply,
        _ => panic!("expected reply"),
    };
    alice_port
        .send_to(&reply.bytes, &reply.destination)
        .unwrap();
    let (n, observed) = bob_port.recv_from(&mut buffer).await.unwrap();
    assert!(matches!(
        bob_routes
            .receive(
                &bob,
                &buffer[..n],
                observed,
                |_| (false, 0),
                &mut bob_frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Confirmed(_)
    ));

    let encoded = bob_routes
        .seal_to(&bob, alice.local_peer(), &frame(b"delivered"))
        .unwrap();
    assert_eq!(encoded.destination, a);
    bob_port
        .send_to(&encoded.bytes, &encoded.destination)
        .unwrap();
    let (n, observed) = alice_port.recv_from(&mut buffer).await.unwrap();
    assert_eq!(observed, b);
    // A received data from B but has not confirmed the reverse route. It must not dispatch it.
    assert!(matches!(
        alice_routes
            .receive(
                &alice,
                &buffer[..n],
                observed,
                |_| (false, 0),
                &mut alice_frames,
                now
            )
            .unwrap(),
        SignedRouteReceive::Probing { .. }
    ));
}

#[test]
fn route_capacity_and_pending_replacement_respect_identity_budget() {
    let alice_id = PeerSigner::from_seed([0x11; 32]).peer_id();
    let charlie = PeerSigner::from_seed([0x33; 32]);
    let charlie_id = charlie.peer_id();
    let bob = boundary(0x22, &[alice_id, charlie_id]);
    let alice = boundary(0x11, &[bob.local_peer()]);
    let charlie = boundary(0x33, &[bob.local_peer()]);
    let mut routes = SignedPeerRoutes::<Slot>::new(1, Duration::from_secs(2));
    let mut frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let now = Instant::now();
    let packet = alice.seal(&frame(b"first")).unwrap();
    let first = match routes
        .receive(&bob, &packet, (1, 1), |_| (false, 0), &mut frames, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("first identity must receive a challenge"),
    };
    assert_eq!(routes.pending.len(), 1);
    assert_eq!(routes.pending.get(&alice_id).unwrap().address, (1, 1));

    // A second signer cannot consume the last slot while the first challenge is live.
    let other = charlie.seal(&frame(b"other")).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &other, (3, 3), |_| (false, 0), &mut frames, now)
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: None,
            ..
        }
    ));
    assert_eq!(routes.pending.len(), 1);
    assert!(!routes.pending.contains_key(&charlie_id));

    // A retransmission from the same route does not change the nonce or emit a challenge.
    let retry = alice.seal(&frame(b"retry")).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &retry, (1, 1), |_| (false, 0), &mut frames, now)
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: None,
            ..
        }
    ));
    assert_eq!(routes.pending.get(&alice_id).unwrap().address, (1, 1));

    // The same signer may challenge a different candidate, replacing its own pending slot.
    let moved = alice.seal(&frame(b"move")).unwrap();
    let second = match routes
        .receive(&bob, &moved, (1, 2), |_| (false, 0), &mut frames, now)
        .unwrap()
    {
        SignedRouteReceive::Probing { challenge, .. } => challenge.unwrap(),
        _ => panic!("same signer must be able to replace its pending route"),
    };
    assert_eq!(second.destination, (1, 2));
    assert_ne!(second.bytes, first.bytes);
    assert_eq!(routes.pending.len(), 1);

    // At the *exact* TTL the previous challenge must expire and free its slot.
    // This distinguishes strict < from <= at the expiration boundary.
    let after_ttl = now + Duration::from_secs(2);
    let other_retry = charlie.seal(&frame(b"retry")).unwrap();
    assert!(matches!(
        routes
            .receive(
                &bob,
                &other_retry,
                (3, 3),
                |_| (false, 0),
                &mut frames,
                after_ttl
            )
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: Some(_),
            ..
        }
    ));
    assert_eq!(routes.pending.len(), 1);
    assert!(routes.pending.contains_key(&charlie_id));
}

#[test]
fn exact_challenge_deadline_is_expired_and_zero_capacity_disables_admission() {
    let (alice, bob) = endpoints();
    let now = Instant::now();
    let mut routes = SignedPeerRoutes::<Slot>::new(0, Duration::from_millis(100));
    let mut frames = Reassembler::<VerifiedPeerId>::for_identity(limits());
    let packet = alice.seal(&frame(b"hello")).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &packet, (1, 1), |_| (false, 0), &mut frames, now)
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: None,
            ..
        }
    ));
    assert!(routes.is_empty());
    assert!(routes.pending.is_empty());

    let mut routes = SignedPeerRoutes::<Slot>::new(1, Duration::from_millis(100));
    let packet = alice.seal(&frame(b"challenge")).unwrap();
    assert!(matches!(
        routes
            .receive(&bob, &packet, (1, 1), |_| (false, 0), &mut frames, now)
            .unwrap(),
        SignedRouteReceive::Probing {
            challenge: Some(_),
            ..
        }
    ));
    let nonce = routes.pending.get(&alice.local_peer()).unwrap().nonce;
    let reply = alice
        .seal(&encode_control(RESPONSE, bob.local_peer(), nonce))
        .unwrap();
    assert!(matches!(
        routes
            .receive(
                &bob,
                &reply,
                (1, 1),
                |_| (false, 0),
                &mut frames,
                now + Duration::from_millis(100)
            )
            .unwrap(),
        SignedRouteReceive::Ignored
    ));
    assert!(routes.is_empty());
    assert!(routes.route(alice.local_peer()).is_none());
}
