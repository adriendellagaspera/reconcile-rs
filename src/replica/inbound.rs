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

/// Why an inbound datagram did not clear the common authentication/admission gate.
///
/// Logging and metrics stay with the caller because full replicas and read replicas expose
/// different operational surfaces. This type owns only the ordering and enough context to report
/// the rejection without re-running a check.
pub(crate) enum InboundRejection {
    Authentication,
    Version(u8),
    PeerCap {
        current_len: usize,
        max: usize,
    },
    Replay {
        seq: Seq,
        stamp: Stamp,
    },
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

    let payload = payload
        .check_version()
        .map_err(InboundRejection::Version)?;

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
