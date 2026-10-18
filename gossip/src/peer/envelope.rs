// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Versioned, opt-in Ed25519 envelope around a legacy sealed datagram.
//!
//! Layout: magic "RCPE" (4) | version (1) | signer PeerId (32) | signature (64)
//! | inner length (u32 little-endian, 4) | complete inner sealed datagram.
//! The signature commits to magic, version, declared length, and all inner bytes.
//! The claimed PeerId must be explicitly authorized and its signature verified.
//! This does not change the live UDP ingress or legacy wire version. Signature
//! verification does not constitute replay or causal-membership admission.

use crate::auth::{Authenticated, Authenticator, Payload, Verified};
use crate::replay::VerifiedPeerReplayFilter;

use super::identity::{PeerAllowlist, PeerProof, PeerSigner, VerifiedPeerId};
use super::PeerId;

/// Magic prefix for signed identity envelopes (not the legacy wire version).
pub const IDENTITY_MAGIC: [u8; 4] = *b"RCPE";
/// Strict outer envelope version, independent of the inner protocol version.
pub const IDENTITY_ENVELOPE_VERSION: u8 = 1;
/// Outer wire header size; this much must be subtracted from a substrate MTU.
pub const IDENTITY_ENVELOPE_OVERHEAD: usize = 4 + 1 + 32 + 64 + 4;

const VERSION: usize = 4;
const ID: usize = VERSION + 1;
const SIGNATURE: usize = ID + 32;
const LENGTH: usize = SIGNATURE + 64;
const PAYLOAD: usize = LENGTH + 4;

/// Why a signed envelope failed to encode, verify, or authenticate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvelopeError {
    /// Outer packet or inner payload exceeds the advertised MTU or u32 length.
    TooLarge,
    /// Truncated packet, declared length mismatch, or trailing bytes.
    InvalidLength,
    /// Not an identity-bearing envelope.
    InvalidMagic,
    /// Unknown and strictly rejected outer envelope version.
    UnsupportedVersion(u8),
    /// Unknown signer, forged identity or invalid signature.
    InvalidProof,
    /// Existing inner MAC/AEAD authentication failed.
    InnerAuthentication,
    /// Existing inner strict wire version is unsupported.
    InnerVersion(u8),
}

fn signed_contents(version: u8, length: [u8; 4], inner: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + 1 + 4 + inner.len());
    bytes.extend_from_slice(&IDENTITY_MAGIC);
    bytes.push(version);
    bytes.extend_from_slice(&length);
    bytes.extend_from_slice(inner);
    bytes
}

/// Wrap an already-sealed full inner datagram with a node-specific signature.
///
/// Caller supplies a strict substrate MTU inclusive of the 105-byte outer envelope;
/// inner framing/recovery must use the correspondingly reduced budget.
pub fn seal(
    signer: &PeerSigner,
    inner: &[u8],
    max_outer_bytes: usize,
) -> Result<Vec<u8>, EnvelopeError> {
    let length = u32::try_from(inner.len()).map_err(|_| EnvelopeError::TooLarge)?;
    let total = IDENTITY_ENVELOPE_OVERHEAD
        .checked_add(inner.len())
        .ok_or(EnvelopeError::TooLarge)?;
    if total > max_outer_bytes {
        return Err(EnvelopeError::TooLarge);
    }
    let length = length.to_le_bytes();
    let proof = signer.sign_datagram(&signed_contents(IDENTITY_ENVELOPE_VERSION, length, inner));
    let mut result = Vec::with_capacity(total);
    result.extend_from_slice(&IDENTITY_MAGIC);
    result.push(IDENTITY_ENVELOPE_VERSION);
    result.extend_from_slice(&proof.claimed_id().to_bytes());
    result.extend_from_slice(&proof.signature_bytes());
    result.extend_from_slice(&length);
    result.extend_from_slice(inner);
    Ok(result)
}

/// Verify an allowlisted individual signer without touching replay or membership state.
///
/// Malformed size/version frames are rejected before Ed25519 verification. In a future
/// signed-only runtime mode, unsigned legacy frames must NOT be silently accepted.
pub fn open<'a>(
    allowlist: &PeerAllowlist,
    frame: &'a [u8],
    max_outer_bytes: usize,
) -> Result<VerifiedEnvelope<'a>, EnvelopeError> {
    if frame.len() > max_outer_bytes {
        return Err(EnvelopeError::TooLarge);
    }
    if frame.len() < IDENTITY_ENVELOPE_OVERHEAD {
        return Err(EnvelopeError::InvalidLength);
    }
    if frame[..4] != IDENTITY_MAGIC {
        return Err(EnvelopeError::InvalidMagic);
    }
    let version = frame[VERSION];
    if version != IDENTITY_ENVELOPE_VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }
    let length: [u8; 4] = frame[LENGTH..PAYLOAD]
        .try_into()
        .expect("fixed-length field");
    let inner = &frame[PAYLOAD..];
    if u32::try_from(inner.len()).ok() != Some(u32::from_le_bytes(length)) {
        return Err(EnvelopeError::InvalidLength);
    }
    let claimed = PeerId::from_bytes(frame[ID..SIGNATURE].try_into().expect("fixed peer key"));
    let signature = frame[SIGNATURE..LENGTH]
        .try_into()
        .expect("fixed signature");
    let proof = PeerProof::from_parts(claimed, signature);
    let peer = allowlist
        .verify(&signed_contents(version, length, inner), &proof)
        .ok_or(EnvelopeError::InvalidProof)?;
    Ok(VerifiedEnvelope { peer, inner })
}

/// A cryptographically verified outer peer envelope, before inner authentication or replay.
pub struct VerifiedEnvelope<'a> {
    peer: VerifiedPeerId,
    inner: &'a [u8],
}

impl<'a> VerifiedEnvelope<'a> {
    /// Return the signature-verified and explicitly allowlisted signer.
    pub fn peer(&self) -> VerifiedPeerId {
        self.peer
    }

    /// Check existing inner cluster MAC/AEAD and strict wire version before replay admission.
    ///
    /// This does not yet invoke the IP-keyed replay filter or modify membership state.
    pub fn authenticate(
        self,
        authenticator: &Authenticator,
    ) -> Result<AuthenticatedEnvelope<'a>, EnvelopeError> {
        let payload = authenticator
            .open(self.inner)
            .ok_or(EnvelopeError::InnerAuthentication)?
            .check_version()
            .map_err(EnvelopeError::InnerVersion)?;
        Ok(AuthenticatedEnvelope {
            peer: self.peer,
            payload,
            has_inner_replay_header: !matches!(authenticator, Authenticator::Disabled),
        })
    }
}

/// Signature and cluster authentication cleared, but replay and peer admission still required.
pub struct AuthenticatedEnvelope<'a> {
    peer: VerifiedPeerId,
    payload: Payload<'a, Authenticated>,
    has_inner_replay_header: bool,
}

impl<'a> AuthenticatedEnvelope<'a> {
    /// Verified logical principal associated with this payload.
    pub fn peer(&self) -> VerifiedPeerId {
        self.peer
    }

    /// Return the existing authenticated payload, still requiring replay/admission checks.
    pub fn into_payload(self) -> Payload<'a, Authenticated> {
        self.payload
    }

    /// Check freshness, sequence and sender capacity under the verified signing identity.
    ///
    /// This *consumes* the signed and inner-authenticated envelope, preserving the binding
    /// between signer and payload. It never accepts a caller-supplied identity or source
    /// endpoint. Missing inner replay metadata (keyless mode) fails closed, even though
    /// signature verification alone succeeded. This does not grant causal membership.
    pub fn verify_replay(
        self,
        filter: &VerifiedPeerReplayFilter,
    ) -> Option<ReplayCheckedEnvelope<'a>> {
        if !self.has_inner_replay_header {
            return None;
        }
        let payload = self
            .payload
            .verify_replay_from_signed_envelope(filter, self.peer)?;
        Some(ReplayCheckedEnvelope {
            peer: self.peer,
            payload,
        })
    }
}

/// An allowlisted, signed payload whose inner authentication, version and replay checks passed.
///
/// This does not authorize a route or an authoritative causal-stability member.
pub struct ReplayCheckedEnvelope<'a> {
    peer: VerifiedPeerId,
    payload: Payload<'a, Verified>,
}

impl<'a> ReplayCheckedEnvelope<'a> {
    /// Identity bound to the checked payload independently of its source endpoint.
    pub fn peer(&self) -> VerifiedPeerId {
        self.peer
    }

    /// Forward the replay-checked payload to a separately policy-gated dispatcher.
    pub fn into_payload(self) -> Payload<'a, Verified> {
        self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ClusterKey, WIRE_VERSION};
    use crate::replay::{
        ReplayFilter, SenderCounter, Seq, Stamp, VerifiedPeerReplayFilter, FRESHNESS_WINDOW_DEFAULT,
    };
    use std::net::IpAddr;

    const MTU: usize = 1200;

    fn alice() -> PeerSigner {
        PeerSigner::from_seed([0x11; 32])
    }

    fn bob() -> PeerSigner {
        PeerSigner::from_seed([0x22; 32])
    }

    fn keyed() -> Authenticator {
        Authenticator::new(Some(ClusterKey::new([0x42; 32])), false).unwrap()
    }

    #[test]
    fn round_trip_keeps_inner_legacy_auth_and_replay_gate_explicit() {
        let signer = alice();
        let auth = keyed();
        let counter = SenderCounter::new();
        let inner = auth.seal(counter.next_seq(), counter.next_stamp(), b"data");
        let frame = seal(&signer, &inner, MTU).unwrap();
        assert_eq!(frame.len(), IDENTITY_ENVELOPE_OVERHEAD + inner.len());
        assert_eq!(&frame[..4], &IDENTITY_MAGIC);
        assert_eq!(frame[VERSION], IDENTITY_ENVELOPE_VERSION);
        assert_eq!(&frame[ID..SIGNATURE], &signer.peer_id().to_bytes());
        assert_eq!(&frame[LENGTH..PAYLOAD], &(inner.len() as u32).to_le_bytes());
        assert_eq!(&frame[PAYLOAD..], inner.as_slice());

        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let outer = open(&allowed, &frame, MTU).unwrap();
        assert_eq!(outer.peer().peer_id(), signer.peer_id());
        let checked = outer.authenticate(&auth).unwrap();
        assert_eq!(checked.peer().peer_id(), signer.peer_id());
        let sender: IpAddr = "127.0.0.1".parse().unwrap();
        let replay = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true);
        let decoded = checked
            .into_payload()
            .verify_replay(&replay, sender)
            .unwrap();
        assert_eq!(decoded.as_bytes(), b"data");
        assert_eq!(replay.len(), 1);
    }

    #[test]
    fn exactly_at_mtu_succeeds_one_byte_over_fails() {
        let signer = alice();
        let inner = vec![7; MTU - IDENTITY_ENVELOPE_OVERHEAD];
        let frame = seal(&signer, &inner, MTU).unwrap();
        assert_eq!(frame.len(), MTU);
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        assert!(open(&allowed, &frame, MTU).is_ok());
        assert_eq!(
            seal(&signer, &[7; MTU - IDENTITY_ENVELOPE_OVERHEAD + 1], MTU).err(),
            Some(EnvelopeError::TooLarge)
        );
        assert!(matches!(
            open(&allowed, &frame, MTU - 1),
            Err(EnvelopeError::TooLarge)
        ));
    }

    #[test]
    fn unknown_signer_and_impersonation_are_rejected() {
        let signed_bob = seal(&bob(), b"inner", MTU).unwrap();
        assert!(matches!(
            open(&PeerAllowlist::new([alice().peer_id()]), &signed_bob, MTU),
            Err(EnvelopeError::InvalidProof)
        ));
        let mut forged = seal(&alice(), b"inner", MTU).unwrap();
        forged[ID..SIGNATURE].copy_from_slice(&bob().peer_id().to_bytes());
        assert!(matches!(
            open(
                &PeerAllowlist::new([alice().peer_id(), bob().peer_id()]),
                &forged,
                MTU
            ),
            Err(EnvelopeError::InvalidProof)
        ));
    }

    #[test]
    fn rejects_tampered_signature_payload_and_length() {
        let signer = alice();
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let original = seal(&signer, b"inner", MTU).unwrap();
        for i in [SIGNATURE, PAYLOAD] {
            let mut frame = original.clone();
            frame[i] ^= 1;
            assert!(matches!(
                open(&allowed, &frame, MTU),
                Err(EnvelopeError::InvalidProof)
            ));
        }
        let mut length = original.clone();
        length[LENGTH] ^= 1;
        assert!(matches!(
            open(&allowed, &length, MTU),
            Err(EnvelopeError::InvalidLength)
        ));
        let mut extra = original.clone();
        extra.push(0);
        assert!(matches!(
            open(&allowed, &extra, MTU),
            Err(EnvelopeError::InvalidLength)
        ));
        assert!(matches!(
            open(&allowed, &original[..original.len() - 1], MTU),
            Err(EnvelopeError::InvalidLength)
        ));
    }

    #[test]
    fn rejects_unknown_outer_version_magic_and_unsigned_legacy() {
        let signer = alice();
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let frame = seal(&signer, b"inner", MTU).unwrap();
        let mut wrong_magic = frame.clone();
        wrong_magic[0] ^= 1;
        assert!(matches!(
            open(&allowed, &wrong_magic, MTU),
            Err(EnvelopeError::InvalidMagic)
        ));
        let mut version = frame.clone();
        version[VERSION] = 2;
        assert!(matches!(
            open(&allowed, &version, MTU),
            Err(EnvelopeError::UnsupportedVersion(2))
        ));
        let legacy = Authenticator::new(None, false).unwrap();
        assert!(legacy.open(&frame).unwrap().check_version().is_err());
        assert!(matches!(
            open(&allowed, &[WIRE_VERSION, 0, 1, 2], MTU),
            Err(EnvelopeError::InvalidLength)
        ));
    }

    #[test]
    fn inner_cluster_mac_and_version_have_distinct_errors() {
        let signer = alice();
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let wrong_key = Authenticator::new(Some(ClusterKey::new([0x44; 32])), false).unwrap();
        let inner = wrong_key.seal(
            crate::replay::Seq::new(1),
            crate::replay::Stamp::new(1),
            b"x",
        );
        let signed = seal(&signer, &inner, MTU).unwrap();
        assert!(matches!(
            open(&allowed, &signed, MTU).unwrap().authenticate(&keyed()),
            Err(EnvelopeError::InnerAuthentication)
        ));

        let disabled = Authenticator::new(None, false).unwrap();
        let inner = [WIRE_VERSION.wrapping_add(1), 1, 2];
        let signed = seal(&signer, &inner, MTU).unwrap();
        assert!(matches!(
            open(&allowed, &signed, MTU)
                .unwrap()
                .authenticate(&disabled),
            Err(EnvelopeError::InnerVersion(_))
        ));
    }

    #[test]
    fn signature_can_verify_repeatedly_without_replay_state() {
        let signer = alice();
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let signed = seal(&signer, b"inner", MTU).unwrap();
        let a = open(&allowed, &signed, MTU).unwrap().peer();
        let b = open(&allowed, &signed, MTU).unwrap().peer();
        assert_eq!(a, b);
        assert_eq!(a.peer_id(), signer.peer_id());
    }

    #[test]
    fn empty_inner_datagram_is_valid_at_exact_outer_header_size() {
        let signer = alice();
        let frame = seal(&signer, b"", IDENTITY_ENVELOPE_OVERHEAD).unwrap();
        assert_eq!(frame.len(), IDENTITY_ENVELOPE_OVERHEAD);
        assert_eq!(&frame[LENGTH..PAYLOAD], &0u32.to_le_bytes());

        let allowed = PeerAllowlist::new([signer.peer_id()]);
        let opened = open(&allowed, &frame, IDENTITY_ENVELOPE_OVERHEAD).unwrap();
        assert_eq!(opened.peer().peer_id(), signer.peer_id());

        // The outer frame is valid, but an empty inner datagram cannot pass its independent
        // protocol-version gate. Do not conflate outer framing with inner admission.
        let disabled = Authenticator::new(None, false).unwrap();
        assert!(matches!(
            opened.authenticate(&disabled),
            Err(EnvelopeError::InnerVersion(0))
        ));
    }

    #[test]
    fn truncated_headers_never_panic() {
        let signer = alice();
        let signed = seal(&signer, b"data", MTU).unwrap();
        let allowed = PeerAllowlist::new([signer.peer_id()]);
        for len in 0..IDENTITY_ENVELOPE_OVERHEAD {
            assert!(matches!(
                open(&allowed, &signed[..len], MTU),
                Err(EnvelopeError::InvalidLength)
            ));
        }
    }

    fn signed_payload(
        signer: &PeerSigner,
        auth: &Authenticator,
        seq: u64,
        stamp: u64,
        data: &[u8],
    ) -> Vec<u8> {
        let inner = auth.seal(Seq::new(seq), Stamp::new(stamp), data);
        seal(signer, &inner, MTU).unwrap()
    }

    fn open_signed<'a>(
        allowlist: &PeerAllowlist,
        auth: &Authenticator,
        frame: &'a [u8],
    ) -> AuthenticatedEnvelope<'a> {
        open(allowlist, frame, MTU)
            .unwrap()
            .authenticate(auth)
            .unwrap()
    }

    #[test]
    fn replay_follows_verified_peer_across_unrelated_source_endpoints() {
        let signer = alice();
        let auth = keyed();
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let filter = VerifiedPeerReplayFilter::new(FRESHNESS_WINDOW_DEFAULT);
        let stamp = chrono::Utc::now().timestamp_millis() as u64;
        let frame = signed_payload(&signer, &auth, 1, stamp, b"payload");
        // The radio/optical source slot is deliberately not an input to replay admission.
        let old_slot = (3_u16, 2_u8);
        let moved_slot = (7_u16, 1_u8);
        assert_ne!(old_slot, moved_slot);

        let checked = open_signed(&allowlist, &auth, &frame)
            .verify_replay(&filter)
            .unwrap();
        assert_eq!(checked.peer().peer_id(), signer.peer_id());
        assert_eq!(checked.into_payload().as_bytes(), b"payload");
        assert_eq!(filter.len(), 1);
        assert!(open_signed(&allowlist, &auth, &frame)
            .verify_replay(&filter)
            .is_none());

        let next = signed_payload(&signer, &auth, 2, stamp, b"after relocation");
        let checked = open_signed(&allowlist, &auth, &next)
            .verify_replay(&filter)
            .unwrap();
        assert_eq!(checked.into_payload().as_bytes(), b"after relocation");
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn independent_signed_peers_share_a_bounded_replay_budget() {
        let a = alice();
        let b = bob();
        let c = PeerSigner::from_seed([0x33; 32]);
        let auth = keyed();
        let allowlist = PeerAllowlist::new([a.peer_id(), b.peer_id(), c.peer_id()]);
        let filter = VerifiedPeerReplayFilter::new(FRESHNESS_WINDOW_DEFAULT).with_max_senders(2);
        let stamp = chrono::Utc::now().timestamp_millis() as u64;

        for signer in [&a, &b] {
            let frame = signed_payload(signer, &auth, 1, stamp, b"first");
            assert!(open_signed(&allowlist, &auth, &frame)
                .verify_replay(&filter)
                .is_some());
        }
        assert_eq!(filter.len(), 2);

        let newcomer = signed_payload(&c, &auth, 1, stamp, b"third");
        assert!(open_signed(&allowlist, &auth, &newcomer)
            .verify_replay(&filter)
            .is_none());
        assert_eq!(filter.len(), 2);

        let a_next = signed_payload(&a, &auth, 2, stamp, b"second");
        assert!(open_signed(&allowlist, &auth, &a_next)
            .verify_replay(&filter)
            .is_some());
        assert!(open_signed(&allowlist, &auth, &a_next)
            .verify_replay(&filter)
            .is_none());

        let zero = VerifiedPeerReplayFilter::new(FRESHNESS_WINDOW_DEFAULT).with_max_senders(0);
        assert!(open_signed(&allowlist, &auth, &newcomer)
            .verify_replay(&zero)
            .is_none());
        assert_eq!(zero.len(), 0);
    }

    #[test]
    fn forged_unknown_stale_or_wrong_key_envelopes_never_allocate_replay_state() {
        let signer = alice();
        let auth = keyed();
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let filter = VerifiedPeerReplayFilter::new(FRESHNESS_WINDOW_DEFAULT);
        let stamp = chrono::Utc::now().timestamp_millis() as u64;
        let legitimate = signed_payload(&signer, &auth, 1, stamp, b"ok");
        let forged = signed_payload(&bob(), &auth, 1, stamp, b"unlisted");
        assert!(matches!(open(&allowlist, &forged, MTU), Err(EnvelopeError::InvalidProof)));
        let mut claim_changed = legitimate.clone();
        claim_changed[ID..SIGNATURE].copy_from_slice(&bob().peer_id().to_bytes());
        assert!(matches!(
            open(&allowlist, &claim_changed, MTU),
            Err(EnvelopeError::InvalidProof)
        ));
        let different_key = Authenticator::new(
            Some(ClusterKey::new([0x78; 32])),
            false,
        ).unwrap();
        assert!(matches!(
            open(&allowlist, &legitimate, MTU)
                .unwrap()
                .authenticate(&different_key),
            Err(EnvelopeError::InnerAuthentication)
        ));
        let stale = signed_payload(&signer, &auth, 1, 1, b"too old");
        assert!(open_signed(&allowlist, &auth, &stale)
            .verify_replay(&filter)
            .is_none());
        assert_eq!(filter.len(), 0);
    }

    #[test]
    fn signed_replay_never_uses_the_keyless_inner_auth_bypass() {
        let signer = alice();
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let keyless = Authenticator::new(None, false).unwrap();
        let frame = seal(&signer, &[WIRE_VERSION, 42], MTU).unwrap();
        let filter = VerifiedPeerReplayFilter::new(FRESHNESS_WINDOW_DEFAULT);
        let checked_inner = open(&allowlist, &frame, MTU)
            .unwrap()
            .authenticate(&keyless)
            .unwrap();
        assert!(checked_inner.verify_replay(&filter).is_none());
        assert_eq!(filter.len(), 0);
    }

}
