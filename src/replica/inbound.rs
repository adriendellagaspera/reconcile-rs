// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;

use gossip::auth;
use gossip::replay::{ReplayFilter, Seq, Stamp};

use super::PeerCap;

impl PeerCap {
    pub(crate) fn new(max_peers: usize) -> Self {
        PeerCap(max_peers)
    }

    pub(crate) fn admits(self, known: bool, current_len: usize) -> bool {
        known || current_len < self.0
    }

    pub(crate) fn max(self) -> usize {
        self.0
    }
}

/// Why an inbound datagram did not clear the common authentication/admission gate.
///
/// Logging and metrics stay with the caller because full replicas and read replicas expose
/// different operational surfaces. This type owns only the ordering and enough context to report
/// the rejection without re-running a check.
#[derive(Debug)]
pub(crate) enum InboundRejection {
    Authentication,
    Version(u8),
    PeerCap { current_len: usize, max: usize },
    Replay { seq: Seq, stamp: Stamp },
}

/// Authenticate, version-check, apply the peer cap, then replay-check one inbound datagram.
///
/// `peer_state` is deliberately lazy: reading membership/peer state happens only after
/// authentication and wire-version validation, preserving the receive-path ordering while letting
/// full and read replicas keep different peer collections.
pub(crate) fn admit_inbound<'a>(
    authenticator: &auth::Authenticator,
    replay_filter: &ReplayFilter,
    max_peers: PeerCap,
    sender: IpAddr,
    datagram: &'a [u8],
    peer_state: impl FnOnce() -> (bool, usize),
) -> Result<auth::Payload<'a, auth::Verified>, InboundRejection> {
    let payload = authenticator
        .open(datagram)
        .ok_or(InboundRejection::Authentication)?;

    let payload = payload.check_version().map_err(InboundRejection::Version)?;

    let (known, current_len) = peer_state();
    if !max_peers.admits(known, current_len) {
        return Err(InboundRejection::PeerCap {
            current_len,
            max: max_peers.max(),
        });
    }

    let (seq, stamp) = (payload.seq, payload.stamp);
    payload
        .verify_replay(replay_filter, sender)
        .ok_or(InboundRejection::Replay { seq, stamp })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gossip::auth::ClusterKey;
    use gossip::replay::{SenderCounter, FRESHNESS_WINDOW_DEFAULT};
    use std::net::{IpAddr, Ipv4Addr};

    fn sender() -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))
    }

    #[test]
    fn authentication_and_version_fail_before_peer_state_is_read() {
        let keyed = auth::Authenticator::new(Some(ClusterKey::new([7; 32])), false).unwrap();
        let filter = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true);

        let auth_rejection = admit_inbound(
            &keyed,
            &filter,
            PeerCap::new(1),
            sender(),
            b"not authenticated",
            || panic!("peer state must not be read before authentication"),
        );
        assert!(matches!(
            auth_rejection,
            Err(InboundRejection::Authentication)
        ));

        let disabled = auth::Authenticator::new(None, false).unwrap();
        let wrong_version = [auth::WIRE_VERSION.wrapping_add(1)];
        let version_rejection = admit_inbound(
            &disabled,
            &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
            PeerCap::new(1),
            sender(),
            &wrong_version,
            || panic!("peer state must not be read before version validation"),
        );
        assert!(matches!(
            version_rejection,
            Err(InboundRejection::Version(_))
        ));
    }

    #[test]
    fn peer_cap_precedes_replay_state_allocation() {
        let authenticator =
            auth::Authenticator::new(Some(ClusterKey::new([7; 32])), false).unwrap();
        let counter = SenderCounter::new();
        let datagram = authenticator.seal(counter.next_seq(), counter.next_stamp(), b"payload");
        let filter = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true);

        let rejected = admit_inbound(
            &authenticator,
            &filter,
            PeerCap::new(1),
            sender(),
            &datagram,
            || (false, 1),
        );
        assert!(matches!(
            rejected,
            Err(InboundRejection::PeerCap {
                current_len: 1,
                max: 1
            })
        ));
        assert_eq!(filter.len(), 0);

        let accepted = admit_inbound(
            &authenticator,
            &filter,
            PeerCap::new(1),
            sender(),
            &datagram,
            || (true, 1),
        );
        assert!(accepted.is_ok());
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn replay_is_the_last_common_gate() {
        let authenticator =
            auth::Authenticator::new(Some(ClusterKey::new([9; 32])), false).unwrap();
        let counter = SenderCounter::new();
        let datagram = authenticator.seal(counter.next_seq(), counter.next_stamp(), b"payload");
        let filter = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true);

        assert!(admit_inbound(
            &authenticator,
            &filter,
            PeerCap::new(1),
            sender(),
            &datagram,
            || (false, 0),
        )
        .is_ok());

        let replayed = admit_inbound(
            &authenticator,
            &filter,
            PeerCap::new(1),
            sender(),
            &datagram,
            || (true, 1),
        );
        assert!(matches!(replayed, Err(InboundRejection::Replay { .. })));
    }
}
