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
mod tests;
