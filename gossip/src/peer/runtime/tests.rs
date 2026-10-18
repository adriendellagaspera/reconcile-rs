// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use crate::auth::{ClusterKey, WIRE_VERSION};
use crate::peer::envelope;
use crate::replay::{SenderCounter, FRESHNESS_WINDOW_DEFAULT};

const MTU: usize = 1200;

fn key() -> Authenticator {
    Authenticator::new(Some(ClusterKey::new([0x55; 32])), false).unwrap()
}

fn signer_a() -> PeerSigner {
    PeerSigner::from_seed([0x11; 32])
}

fn signer_b() -> PeerSigner {
    PeerSigner::from_seed([0x22; 32])
}

fn boundary(
    signer: PeerSigner,
    allowed: impl IntoIterator<Item = PeerId>,
    capacity: usize,
) -> SignedDatagramBoundary {
    SignedDatagramBoundary::new(
        key(),
        signer,
        PeerAllowlist::new(allowed),
        FRESHNESS_WINDOW_DEFAULT,
        2,
        capacity,
        MTU,
    )
    .unwrap()
}

#[test]
fn signed_exchange_checks_identity_without_binding_replay_to_source_address() {
    let a = boundary(signer_a(), [signer_b().peer_id()], 1);
    let b = boundary(signer_b(), [signer_a().peer_id()], 1);
    let first = a.seal(b"dated-1").unwrap();
    let source_one = (3_u16, 4_u8);
    let source_two = (8_u16, 2_u8);
    assert_ne!(source_one, source_two);

    let accepted = b.open(&first, |peer| {
        assert_eq!(peer, a.local_peer());
        (false, 0)
    }).unwrap();
    assert_eq!(accepted.peer().peer_id(), a.local_peer());
    assert_eq!(accepted.into_payload().as_bytes(), b"dated-1");

    assert_eq!(b.open(&first, |_| (true, 1)).err(), Some(SignedDatagramError::Replay));
    assert_eq!(b.replay_senders(), 1);
    let second = a.seal(b"dated-after-route-change").unwrap();
    let accepted = b.open(&second, |peer| {
        assert_eq!(peer, a.local_peer());
        (true, 1)
    }).unwrap();
    assert_eq!(accepted.into_payload().as_bytes(), b"dated-after-route-change");
    assert_eq!(b.replay_senders(), 1);
}

#[test]
fn unknown_signer_unsigned_frames_and_tampering_never_consult_member_state() {
    let a = boundary(signer_a(), [signer_b().peer_id()], 1);
    let b = boundary(signer_b(), [], 1);
    let signed_b = b.seal(b"unlisted").unwrap();
    assert_eq!(
        a.open(&signed_b, |_| panic!("untrusted claim must not consult membership")).err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::InvalidProof))
    );

    let unsigned_inner = key().seal(
        SenderCounter::new().next_seq(),
        SenderCounter::new().next_stamp(),
        b"legacy",
    );
    assert_eq!(
        a.open(&unsigned_inner, |_| panic!("unsigned frame must not touch topology")).err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::InvalidLength))
    );

    let signed_a = a.seal(b"payload").unwrap();
    let mut forged = signed_a.clone();
    forged[envelope::IDENTITY_ENVELOPE_OVERHEAD] ^= 1;
    let receiver = boundary(signer_b(), [signer_a().peer_id()], 1);
    assert_eq!(
        receiver.open(&forged, |_| panic!("forged datagram must not reach membership")).err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::InvalidProof))
    );
    assert_eq!(receiver.replay_senders(), 0);
    assert!(receiver.open(&signed_a, |_| (false, 0)).is_ok());
}

#[test]
fn peer_cap_is_checked_before_allocating_replay_state() {
    let a = boundary(signer_a(), [], 1);
    let b = boundary(signer_b(), [a.local_peer()], 1);
    let packet = a.seal(b"dated").unwrap();
    assert_eq!(
        b.open(&packet, |_| (false, 1)).err(),
        Some(SignedDatagramError::PeerCap { current: 1, maximum: 1 })
    );
    assert_eq!(b.replay_senders(), 0);
    assert!(b.open(&packet, |_| (true, 1)).is_ok());
    assert_eq!(b.replay_senders(), 1);
    assert_eq!(b.open(&packet, |_| (true, 1)).err(), Some(SignedDatagramError::Replay));
}

#[test]
fn disabled_inner_authenticator_is_rejected_before_creating_a_signed_boundary() {
    let result = SignedDatagramBoundary::new(
        Authenticator::Disabled,
        signer_a(),
        PeerAllowlist::new([signer_b().peer_id()]),
        FRESHNESS_WINDOW_DEFAULT,
        100,
        100,
        MTU,
    );
    assert!(matches!(
        result,
        Err(SignedDatagramError::MissingInnerAuthentication)
    ));
}

#[test]
fn outer_mtu_budget_is_respected_on_send_and_receive() {
    let sender = signer_a();
    let recipient = boundary(signer_b(), [sender.peer_id()], 2);
    let a = boundary(sender, [], 2);
    let raw = vec![0x37; a.payload_budget()];
    let maximum = a.seal(&raw).unwrap();
    assert_eq!(maximum.len(), MTU);
    assert_eq!(recipient.open(&maximum, |_| (false, 0)).unwrap().into_payload().as_bytes(), raw);

    assert_eq!(
        a.seal(&vec![0x37; a.payload_budget() + 1]).err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::TooLarge))
    );
    let too_small = SignedDatagramBoundary::new(
        key(),
        signer_a(),
        PeerAllowlist::new([]),
        FRESHNESS_WINDOW_DEFAULT,
        1,
        1,
        envelope::IDENTITY_ENVELOPE_OVERHEAD + key().overhead(),
    );
    assert!(matches!(too_small, Err(SignedDatagramError::MtuTooSmall)));
    assert_eq!(maximum[4], envelope::IDENTITY_ENVELOPE_VERSION);
}

#[test]
fn invalid_inner_mac_precedes_peer_cap_and_replay() {
    let signer = signer_a();
    let inner = key().seal(
        SenderCounter::new().next_seq(),
        SenderCounter::new().next_stamp(),
        b"data",
    );
    let mut incorrect_inner = inner.clone();
    let offset = crate::auth::TAG_LEN + crate::replay::REPLAY_HEADER_LEN;
    incorrect_inner[offset] = WIRE_VERSION.wrapping_add(1);
    // Re-signing cannot repair a modified inner wire version without a matching MAC.
    let valid_signature_bad_inner_mac = envelope::seal(&signer, &incorrect_inner, MTU).unwrap();
    let receiver = boundary(signer_b(), [signer.peer_id()], 1);
    assert_eq!(
        receiver.open(&valid_signature_bad_inner_mac, |_| panic!("inner MAC rejected first")).err(),
        Some(SignedDatagramError::Envelope(EnvelopeError::InnerAuthentication))
    );
    assert_eq!(receiver.replay_senders(), 0);
}
