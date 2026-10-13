// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Opaque logical identity, distinct from transport endpoints and the LWW clock's `NodeId`.
//!
//! A `PeerId` value **does not authenticate its holder**. A sender-supplied claim must not be
//! admitted to replay, membership, or causal-GC state without independently verified binding.
//! Deployed UDP traffic and durable state still use IP addresses. The optional identity proof
//! verifier authenticates datagram ownership, but is not yet wired into the runtime.

pub mod identity;

/// A 256-bit, substrate-independent logical peer identifier.
///
/// Equality and hashing depend only on these bytes, never on an IP address, port, or datalink
/// endpoint. Provision identities uniquely and persist them across restarts; generating a fresh
/// ID whenever a transport endpoint changes would defeat the intended stable-identity model.
///
/// `PeerId` is a *label*, not a credential or verification result. In particular, a cluster-wide
/// shared MAC key cannot distinguish the individual nodes that claim these bytes. Callers must
/// not treat `PeerId::from_bytes` as evidence of authentication.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Construct a logical identity from provisioned bytes, without authenticating them.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the original, fixed-width identity bytes without attaching endpoint information.
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use super::PeerId;

    #[test]
    fn identities_have_stable_value_semantics() {
        let a = PeerId::from_bytes([0x11; 32]);
        let a_again = PeerId::from_bytes([0x11; 32]);
        let b = PeerId::from_bytes([0x22; 32]);

        assert_eq!(a.to_bytes(), [0x11; 32]);
        assert_eq!(a, a_again);
        assert_ne!(a, b);
        assert_eq!(BTreeSet::from([b, a_again, a]).len(), 2);

        let routes = HashMap::from([(a, "optical-port-2")]);
        assert_eq!(routes.get(&a_again), Some(&"optical-port-2"));
        assert!(!routes.contains_key(&b));
    }

    #[test]
    fn changing_a_route_does_not_change_peer_identity() {
        let peer = PeerId::from_bytes([0xa5; 32]);
        let mut directory = HashMap::from([(peer, (3_u16, 2_u8))]);
        directory.insert(peer, (3_u16, 7_u8));

        assert_eq!(directory.len(), 1);
        assert_eq!(directory.get(&peer), Some(&(3, 7)));
        assert_eq!(peer.to_bytes(), [0xa5; 32]);
    }
}
