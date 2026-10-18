// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Strict, opt-in signed datagram boundary for the future identity-keyed runtime.
//!
//! An allowlisted signing identity is authenticated before peer-cap lookup; replay is always
//! checked by that identity before the protocol dispatcher sees the payload. This boundary
//! authenticates packets, **not** routes or authoritative causal membership. A caller must
//! still parse a complete dated message before recording a member or tombstone ACK.

use std::time::Duration;

use crate::auth::Authenticator;
use crate::replay::{SenderCounter, VerifiedPeerReplayFilter};

use super::envelope::{self, EnvelopeError, ReplayCheckedEnvelope, IDENTITY_ENVELOPE_OVERHEAD};
use super::identity::{PeerAllowlist, PeerSigner};
use super::PeerId;

/// Why a signed datagram could not be admitted or emitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignedDatagramError {
    /// A cluster key is required to carry the authenticated sequence/stamp replay header.
    MissingInnerAuthentication,
    /// The substrate MTU cannot carry even the required signed/authenticated headers.
    MtuTooSmall,
    /// Signature, allowlist, framing, MAC or wire-version check failed.
    Envelope(EnvelopeError),
    /// New signers are refused at the authoritative topology capacity limit.
    PeerCap { current: usize, maximum: usize },
    /// The signed identity's replay window rejects a duplicate, stale, or excessive sender.
    Replay,
}

/// A signed-only ingress/egress policy that never falls back to legacy unsigned datagrams.
///
/// The same local signer is used for all outgoing datagrams. Each incoming signer must appear
/// in the explicitly provisioned allowlist and prove ownership of the complete packet. The
/// caller supplies authoritative *logical* member counts, never counts by source address.
pub struct SignedDatagramBoundary {
    authenticator: Authenticator,
    signer: PeerSigner,
    allowlist: PeerAllowlist,
    counter: SenderCounter,
    replay: VerifiedPeerReplayFilter,
    max_peers: usize,
    mtu: usize,
}

impl SignedDatagramBoundary {
    /// Configure one signed-only endpoint, with a strict datagram budget including both headers.
    ///
    /// An unauthenticated inner frame has no sequence/stamp and cannot safely use signed replay;
    /// it is rejected at construction rather than silently admitting a downgrade.
    ///
    /// # Errors
    /// If the inner authenticator is disabled or the MTU cannot fit both framing layers.
    pub fn new(
        authenticator: Authenticator,
        signer: PeerSigner,
        allowlist: PeerAllowlist,
        freshness_window: Duration,
        max_replay_senders: usize,
        max_peers: usize,
        mtu: usize,
    ) -> Result<Self, SignedDatagramError> {
        if matches!(authenticator, Authenticator::Disabled) {
            return Err(SignedDatagramError::MissingInnerAuthentication);
        }
        if mtu <= IDENTITY_ENVELOPE_OVERHEAD + authenticator.overhead() {
            return Err(SignedDatagramError::MtuTooSmall);
        }
        Ok(Self {
            authenticator,
            signer,
            allowlist,
            counter: SenderCounter::new(),
            replay: VerifiedPeerReplayFilter::new(freshness_window)
                .with_max_senders(max_replay_senders),
            max_peers,
            mtu,
        })
    }

    /// The local Ed25519 identity, independent of the datagram route.
    pub fn local_peer(&self) -> PeerId {
        self.signer.peer_id()
    }

    /// Maximum protocol bytes after the outer signature and inner authentication headers.
    ///
    /// Framing and fragment recovery must use this smaller budget, not the UDP legacy budget.
    pub fn payload_budget(&self) -> usize {
        self.mtu - IDENTITY_ENVELOPE_OVERHEAD - self.authenticator.overhead()
    }

    /// Seal the complete inner packet, then sign it within the substrate MTU.
    ///
    /// # Errors
    /// If the protocol messages exceed the advertised datagram payload budget.
    pub fn seal(&self, protocol_messages: &[u8]) -> Result<Vec<u8>, SignedDatagramError> {
        if protocol_messages.len() > self.payload_budget() {
            return Err(SignedDatagramError::Envelope(EnvelopeError::TooLarge));
        }
        let inner = self.authenticator.seal(
            self.counter.next_seq(),
            self.counter.next_stamp(),
            protocol_messages,
        );
        envelope::seal(&self.signer, &inner, self.mtu).map_err(SignedDatagramError::Envelope)
    }

    /// Admit one signed datagram without consulting an untrusted transport address.
    ///
    /// First verify full Ed25519 proof and allowlist; then the inner MAC and strict wire version.
    /// Only then inspect authoritative member state and enforce the peer cap, followed by replay.
    /// Caller must separately establish a valid dated channel and route/returnability policy.
    ///
    /// # Errors
    /// On any failed check. There is no legacy unsigned acceptance path.
    pub fn open<'a>(
        &self,
        datagram: &'a [u8],
        peer_state: impl FnOnce(PeerId) -> (bool, usize),
    ) -> Result<ReplayCheckedEnvelope<'a>, SignedDatagramError> {
        let verified = envelope::open(&self.allowlist, datagram, self.mtu)
            .map_err(SignedDatagramError::Envelope)?
            .authenticate(&self.authenticator)
            .map_err(SignedDatagramError::Envelope)?;
        let peer = verified.peer().peer_id();
        let (known, count) = peer_state(peer);
        if !known && count >= self.max_peers {
            return Err(SignedDatagramError::PeerCap {
                current: count,
                maximum: self.max_peers,
            });
        }
        verified
            .verify_replay(&self.replay)
            .ok_or(SignedDatagramError::Replay)
    }

    /// Number of tracked *signing identities*, not source endpoints.
    pub fn replay_senders(&self) -> usize {
        self.replay.len()
    }
}

#[cfg(test)]
mod tests;
