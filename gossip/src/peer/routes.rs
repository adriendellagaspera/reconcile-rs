// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A signed, route-confirmed ingress/egress adapter independent of IP and link-layer addresses.
//!
//! A signed packet proves identity, but not that the claimed source is a returnable route.
//! Protocol frames from an unconfirmed route are therefore *not dispatched*: a random
//! challenge must return from that exact route under the same verified signer before any
//! reassembly or outbound protocol traffic is permitted. No routing observation grants
//! causal membership or a tombstone acknowledgement.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rand::{rngs::OsRng, Rng};

use crate::framing::{Reassembler, ReceiveReport};
use crate::peer::identity::VerifiedPeerId;
use crate::peer::runtime::{SignedDatagramBoundary, SignedDatagramError};
use crate::peer::PeerId;

const CHALLENGE: u8 = 0xf0;
const RESPONSE: u8 = 0xf1;
const CONTROL_LEN: usize = 1 + 32 + 32;

/// One signed datagram addressed to a substrate endpoint, which need not be an IP address.
pub struct RoutedDatagram<A> {
    /// Destination from a confirmed route, or an explicitly authenticated challenge exchange.
    pub destination: A,
    /// Full wire datagram, including Ed25519 signature and authenticated replay header.
    pub bytes: Vec<u8>,
}

/// Outcome of a signed and replay-checked packet, before any causal-membership mutation.
pub enum SignedRouteReceive<'a, A> {
    /// Valid protocol frame from a confirmed route; the message still requires dated decoding.
    Framed {
        /// Individually verified signer.
        peer: VerifiedPeerId,
        /// Bounded fragment reassembly result.
        report: ReceiveReport<'a>,
    },
    /// Authenticated signer using an unconfirmed source address. No protocol state was changed.
    Probing {
        /// Individually verified signer.
        peer: VerifiedPeerId,
        /// New returnability challenge, if one was not already outstanding.
        challenge: Option<RoutedDatagram<A>>,
    },
    /// Answer a valid signed challenge, without admitting the peer as a causal member.
    Reply(RoutedDatagram<A>),
    /// The exact outstanding challenge was returned by the expected signer and source route.
    Confirmed(VerifiedPeerId),
    /// Malformed, misdirected, expired or unsolicited route-control frame.
    Ignored,
}

/// Failure before a signed packet can be used for any routing or protocol state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignedRouteError {
    /// The packet failed authentication, allowlist, peer cap, replay, or MTU admission.
    Signed(SignedDatagramError),
    /// No returnability-confirmed route is available for this target identity.
    NoConfirmedRoute,
}

impl From<SignedDatagramError> for SignedRouteError {
    fn from(value: SignedDatagramError) -> Self {
        Self::Signed(value)
    }
}

struct Pending<A> {
    address: A,
    nonce: [u8; 32],
    issued: Instant,
}

/// A bounded, independently authenticated peer→route directory.
///
/// Address observations cannot mutate the confirmed route. Only a reply signed by the
/// expected peer, echoing a cryptographically random nonce from the exact proposed route,
/// can promote a route. Existing routes are kept usable while a replacement is challenged.
/// All expiration times are monotonic; explicit decommission removes both route and challenge.
pub struct SignedPeerRoutes<A> {
    confirmed: HashMap<PeerId, A>,
    pending: HashMap<PeerId, Pending<A>>,
    max_peers: usize,
    challenge_ttl: Duration,
}

impl<A: Clone + Eq> SignedPeerRoutes<A> {
    /// Make an empty directory with independent bounds on confirmed and outstanding identities.
    /// A zero peer budget disables new route admission.
    pub fn new(max_peers: usize, challenge_ttl: Duration) -> Self {
        Self {
            confirmed: HashMap::new(),
            pending: HashMap::new(),
            max_peers,
            challenge_ttl,
        }
    }

    /// Look up only a challenge-confirmed endpoint. A mere signed observation is never returned.
    pub fn route(&self, peer: PeerId) -> Option<&A> {
        self.confirmed.get(&peer)
    }

    /// Number of independently confirmed signers, not addresses.
    pub fn len(&self) -> usize {
        self.confirmed.len()
    }

    /// Whether any confirmed signer is present.
    pub fn is_empty(&self) -> bool {
        self.confirmed.is_empty()
    }

    /// Explicitly forget both routability and outstanding proof when a member is decommissioned.
    pub fn decommission(&mut self, peer: PeerId) {
        self.confirmed.remove(&peer);
        self.pending.remove(&peer);
    }

    /// Seal one frame for an explicitly confirmed signer. Does not infer a route from packet origin.
    ///
    /// # Errors
    /// If the destination is unconfirmed or the signed datagram exceeds the configured MTU.
    pub fn seal_to(
        &self,
        boundary: &SignedDatagramBoundary,
        peer: PeerId,
        frame: &[u8],
    ) -> Result<RoutedDatagram<A>, SignedRouteError> {
        let destination = self
            .route(peer)
            .ok_or(SignedRouteError::NoConfirmedRoute)?
            .clone();
        Ok(RoutedDatagram {
            destination,
            bytes: boundary.seal(frame)?,
        })
    }

    /// Admit a signed datagram and, only after returnability confirmation, reassemble its frame.
    ///
    /// `source` is untrusted endpoint metadata; `peer_state` is the caller's authoritative
    /// causal-member capacity lookup and is invoked only after signature and MAC validation.
    ///
    /// # Errors
    /// If cryptographic admission, anti-replay or a required signed response fails.
    pub fn receive<'a>(
        &mut self,
        boundary: &SignedDatagramBoundary,
        datagram: &'a [u8],
        source: A,
        peer_state: impl FnOnce(PeerId) -> (bool, usize),
        reassembler: &mut Reassembler<VerifiedPeerId>,
        now: Instant,
    ) -> Result<SignedRouteReceive<'a, A>, SignedRouteError> {
        let checked = boundary.open(datagram, peer_state)?;
        let peer = checked.peer();
        let bytes = checked.into_payload();
        let frame = bytes.as_bytes();
        match frame.first().copied() {
            Some(CHALLENGE) => {
                let Some((recipient, nonce)) = parse_control(frame) else {
                    return Ok(SignedRouteReceive::Ignored);
                };
                if recipient != boundary.local_peer() {
                    return Ok(SignedRouteReceive::Ignored);
                }
                let reply = encode_control(RESPONSE, peer.peer_id(), nonce);
                Ok(SignedRouteReceive::Reply(RoutedDatagram {
                    destination: source,
                    bytes: boundary.seal(&reply)?,
                }))
            }
            Some(RESPONSE) => {
                let Some((recipient, nonce)) = parse_control(frame) else {
                    return Ok(SignedRouteReceive::Ignored);
                };
                if recipient != boundary.local_peer() {
                    return Ok(SignedRouteReceive::Ignored);
                }
                let id = peer.peer_id();
                let matches = self.pending.get(&id).is_some_and(|candidate| {
                    candidate.address == source
                        && candidate.nonce == nonce
                        && now.saturating_duration_since(candidate.issued) < self.challenge_ttl
                });
                if !matches {
                    return Ok(SignedRouteReceive::Ignored);
                }
                self.pending.remove(&id);
                self.confirmed.insert(id, source);
                Ok(SignedRouteReceive::Confirmed(peer))
            }
            _ => {
                if self.confirmed.get(&peer.peer_id()) == Some(&source) {
                    let report = reassembler.accept(peer, bytes, now);
                    return Ok(SignedRouteReceive::Framed { peer, report });
                }
                let challenge = self.start_probe(boundary, peer, source, now)?;
                Ok(SignedRouteReceive::Probing { peer, challenge })
            }
        }
    }

    fn start_probe(
        &mut self,
        boundary: &SignedDatagramBoundary,
        peer: VerifiedPeerId,
        source: A,
        now: Instant,
    ) -> Result<Option<RoutedDatagram<A>>, SignedRouteError> {
        self.pending.retain(|_, candidate| {
            now.saturating_duration_since(candidate.issued) < self.challenge_ttl
        });
        let id = peer.peer_id();
        if self
            .pending
            .get(&id)
            .is_some_and(|candidate| candidate.address == source)
        {
            return Ok(None);
        }
        if !self.pending.contains_key(&id)
            && !self.confirmed.contains_key(&id)
            && self.confirmed.len() + self.pending.len() >= self.max_peers
        {
            return Ok(None);
        }
        let nonce: [u8; 32] = OsRng.gen();
        let bytes = boundary.seal(&encode_control(CHALLENGE, id, nonce))?;
        self.pending.insert(
            id,
            Pending {
                address: source.clone(),
                nonce,
                issued: now,
            },
        );
        Ok(Some(RoutedDatagram {
            destination: source,
            bytes,
        }))
    }
}

fn encode_control(kind: u8, recipient: PeerId, nonce: [u8; 32]) -> [u8; CONTROL_LEN] {
    let mut bytes = [0u8; CONTROL_LEN];
    bytes[0] = kind;
    bytes[1..33].copy_from_slice(&recipient.to_bytes());
    bytes[33..].copy_from_slice(&nonce);
    bytes
}

fn parse_control(bytes: &[u8]) -> Option<(PeerId, [u8; 32])> {
    if bytes.len() != CONTROL_LEN {
        return None;
    }
    let id = PeerId::from_bytes(bytes[1..33].try_into().ok()?);
    let nonce = bytes[33..].try_into().ok()?;
    Some((id, nonce))
}

#[cfg(test)]
mod tests;
