// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Cryptographic ownership proofs for allowlisted logical peer identities.
//!
//! `PeerId` is the exact 32-byte Ed25519 public key of an enrolled signer. The receiver
//! checks both an explicit allowlist and a signature over the *complete* datagram.
//! A public key alone, or the cluster-wide MAC key, does not establish this proof.
//!
//! This is an independent primitive, **not yet a wire format or an ingress integration**.
//! After verification, the caller must still enforce the existing datagram freshness,
//! per-sender replay window, protocol version, resource admission, and membership policy.
//! The same signed datagram can be replayed unless those checks are performed. In particular,
//! do not record a `VerifiedPeerId` as a causal member just because a signature is valid.

use std::collections::HashSet;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

use super::PeerId;

/// Versioned signature domain for complete application wire datagrams.
///
/// Domain separation prevents signatures made for another use of the Ed25519 key from being
/// valid peer proofs. The framing commits to datagram length as well as contents.
const SIGNATURE_DOMAIN: &[u8] = b"reconcile/gossip/peer-datagram/v1\0";

fn signed_bytes(datagram: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(SIGNATURE_DOMAIN.len() + 8 + datagram.len());
    message.extend_from_slice(SIGNATURE_DOMAIN);
    message.extend_from_slice(&(datagram.len() as u64).to_le_bytes());
    message.extend_from_slice(datagram);
    message
}

/// A signer holding a node-specific Ed25519 secret.
///
/// Provision the 32-byte private seed securely and retain it across restarts. Never derive it
/// from the shared cluster PSK, publish it, or use an ephemeral per-startup seed when a stable
/// identity is required. Secret material is zeroized on drop by the crypto implementation.
pub struct PeerSigner(SigningKey);

impl PeerSigner {
    /// Create a signer from a securely provisioned Ed25519 secret seed.
    ///
    /// This API does **not** generate keys: provisioning and durable secure storage are the
    /// operator's responsibility.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self(SigningKey::from_bytes(&seed))
    }

    /// Stable logical identity derived from the signer's public verification key.
    pub fn peer_id(&self) -> PeerId {
        PeerId::from_bytes(self.0.verifying_key().to_bytes())
    }

    /// Sign the *complete* serialized wire datagram, including authenticated replay metadata.
    ///
    /// Signs data exactly as supplied: the caller must not omit headers or later mutate bytes.
    pub fn sign_datagram(&self, datagram: &[u8]) -> PeerProof {
        let signature = self.0.sign(&signed_bytes(datagram)).to_bytes();
        PeerProof::from_parts(self.peer_id(), signature)
    }
}

/// An **untrusted** identity claim and detached signature, suitable for a future wire envelope.
///
/// Constructing this value does not prove possession of the key. The claim must be checked
/// against an explicit `PeerAllowlist` over the original complete datagram bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerProof {
    claimed: PeerId,
    signature: [u8; 64],
}

impl PeerProof {
    /// Reconstruct an untrusted proof from wire-sized fields.
    pub const fn from_parts(claimed: PeerId, signature: [u8; 64]) -> Self {
        Self { claimed, signature }
    }

    /// The sender-claimed, *unverified* peer identifier.
    pub const fn claimed_id(&self) -> PeerId {
        self.claimed
    }

    /// The raw detached signature bytes.
    pub const fn signature_bytes(&self) -> [u8; 64] {
        self.signature
    }
}

/// A peer identity established by a valid signature **and** explicit authorization.
///
/// This is only a proof of ownership for the supplied datagram bytes. It does not prove that
/// those bytes are fresh, that their sequence is new, or that the peer is eligible for causal
/// membership. Unlike `PeerId`, the verified wrapper has no public unchecked constructor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VerifiedPeerId(PeerId);

impl VerifiedPeerId {
    /// Return the authorized identity after cryptographic verification.
    pub const fn peer_id(self) -> PeerId {
        self.0
    }
}

/// Static authorization policy for signer public keys.
///
/// Only explicitly provisioned peer IDs can pass verification; a stranger signing its own
/// datagram with a new valid Ed25519 key is **not** admitted. The allowlist is populated only
/// through trusted configuration, never from the network.
#[derive(Debug, Default)]
pub struct PeerAllowlist {
    allowed: HashSet<PeerId>,
}

impl PeerAllowlist {
    /// Provision a set of independently authorized peer identities.
    pub fn new(identities: impl IntoIterator<Item = PeerId>) -> Self {
        Self {
            allowed: identities.into_iter().collect(),
        }
    }

    /// Verify that an authorized signer controls the claimed identity and signed these bytes.
    ///
    /// Returns `None` for a non-allowlisted identity, a malformed public key, a mismatched
    /// signature, or a modified datagram. This method does not mutate replay state or any
    /// membership table.
    pub fn verify(&self, datagram: &[u8], proof: &PeerProof) -> Option<VerifiedPeerId> {
        if !self.allowed.contains(&proof.claimed) {
            return None;
        }
        let key = VerifyingKey::from_bytes(&proof.claimed.to_bytes()).ok()?;
        let signature = Signature::from_bytes(&proof.signature);
        key.verify_strict(&signed_bytes(datagram), &signature).ok()?;
        Some(VerifiedPeerId(proof.claimed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice() -> PeerSigner {
        PeerSigner::from_seed([0x11; 32])
    }

    fn bob() -> PeerSigner {
        PeerSigner::from_seed([0x22; 32])
    }

    #[test]
    fn explicitly_authorized_signature_binds_claim_to_full_datagram() {
        let signer = alice();
        let datagram = b"wire-version | sequence | timestamp | mac | payload";
        let proof = signer.sign_datagram(datagram);
        let allowed = PeerAllowlist::new([signer.peer_id()]);

        assert_eq!(proof.claimed_id(), signer.peer_id());
        assert!(proof.signature_bytes().iter().any(|&x| x != 0));
        assert_eq!(
            allowed.verify(datagram, &proof).map(VerifiedPeerId::peer_id),
            Some(signer.peer_id())
        );
    }

    #[test]
    fn unknown_signer_is_not_enrolled_by_valid_signature() {
        let attacker = bob();
        let proof = attacker.sign_datagram(b"payload");
        let allowlist = PeerAllowlist::new([alice().peer_id()]);
        assert!(allowlist.verify(b"payload", &proof).is_none());
        assert!(PeerAllowlist::default().verify(b"payload", &proof).is_none());
    }

    #[test]
    fn cannot_impersonate_a_different_authorized_peer() {
        let a = alice();
        let b = bob();
        let signed = a.sign_datagram(b"payload");
        let claim_b = PeerProof::from_parts(b.peer_id(), signed.signature_bytes());
        let allowlist = PeerAllowlist::new([a.peer_id(), b.peer_id()]);

        assert!(allowlist.verify(b"payload", &claim_b).is_none());
        assert_eq!(
            allowlist.verify(b"payload", &signed).map(VerifiedPeerId::peer_id),
            Some(a.peer_id())
        );
    }

    #[test]
    fn modified_header_and_payload_are_rejected() {
        let signer = alice();
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let message = b"version-1 | seq-7 | payload";
        let proof = signer.sign_datagram(message);
        assert!(allowlist.verify(message, &proof).is_some());
        assert!(allowlist.verify(b"version-2 | seq-7 | payload", &proof).is_none());
        assert!(allowlist.verify(b"version-1 | seq-8 | payload", &proof).is_none());
        assert!(allowlist.verify(b"version-1 | seq-7 | PAYLOAD", &proof).is_none());
        assert!(allowlist.verify(b"version-1 | seq-7 | payload!", &proof).is_none());
    }

    #[test]
    fn corrupted_signature_and_noncanonical_public_key_are_rejected() {
        let signer = alice();
        let allowlist = PeerAllowlist::new([signer.peer_id(), PeerId::from_bytes([0xff; 32])]);
        let proof = signer.sign_datagram(b"payload");
        let mut sig = proof.signature_bytes();
        sig[0] ^= 0x01;
        let tampered = PeerProof::from_parts(signer.peer_id(), sig);
        assert!(allowlist.verify(b"payload", &tampered).is_none());
        let invalid = PeerProof::from_parts(PeerId::from_bytes([0xff; 32]), proof.signature_bytes());
        assert!(allowlist.verify(b"payload", &invalid).is_none());
    }

    #[test]
    fn endpoint_moves_do_not_change_cryptographic_identity() {
        let signer = alice();
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let proof = signer.sign_datagram(b"datagram");
        // Source address is not included in the identity proof: a separately authenticated
        // route update may relocate this node, but replay history must follow its peer ID.
        let old_route = (3_u16, 2_u8);
        let new_route = (3_u16, 7_u8);
        assert_ne!(old_route, new_route);
        assert_eq!(
            allowlist.verify(b"datagram", &proof).unwrap().peer_id(),
            signer.peer_id()
        );
    }

    #[test]
    fn replay_is_not_automatically_prevented_by_signature_verification() {
        let signer = alice();
        let proof = signer.sign_datagram(b"identical datagram");
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        let first = allowlist.verify(b"identical datagram", &proof);
        let second = allowlist.verify(b"identical datagram", &proof);
        assert_eq!(first, second);
        assert_eq!(first.map(VerifiedPeerId::peer_id), Some(signer.peer_id()));
        // Future ingress must reject the second via replay state keyed by VerifiedPeerId.
    }

    #[test]
    fn proof_round_trip_retains_untrusted_claim_and_signature() {
        let signer = alice();
        let proof = signer.sign_datagram(b"message");
        let reconstructed = PeerProof::from_parts(proof.claimed_id(), proof.signature_bytes());
        let allowlist = PeerAllowlist::new([signer.peer_id()]);
        assert_eq!(proof, reconstructed);
        assert!(allowlist.verify(b"message", &reconstructed).is_some());
    }
}
