// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;

use gossip::auth::{self, Authenticator};
use gossip::replay::{ReplayFilter, Seq, Stamp};

use super::PeerCap;

/// Why an inbound datagram did not clear the shared authentication/admission gate.
pub(crate) enum DatagramRejection {
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

/// Authenticate, version-check, bound peer state, then verify replay metadata.
///
/// Callers decide what "known peer" means and keep their own logging/metrics/bookkeeping. This
/// function owns only the security-sensitive ordering shared by full and read replicas.
pub(crate) fn admit_datagram<'a>(
    authenticator: &Authenticator,
    replay_filter: &ReplayFilter,
    max_peers: PeerCap,
    sender: IpAddr,
    peer_state: impl FnOnce() -> (bool, usize),
    datagram: &'a [u8],
) -> Result<auth::Payload<'a, auth::Verified>, DatagramRejection> {
    let payload = authenticator
        .open(datagram)
        .ok_or(DatagramRejection::Authentication)?;
    let payload = payload
        .check_version()
        .map_err(DatagramRejection::Version)?;

    let (known, current_len) = peer_state();
    if !max_peers.admits(known, current_len) {
        return Err(DatagramRejection::PeerCap {
            current_len,
            max: max_peers.max(),
        });
    }

    let (seq, stamp) = (payload.seq, payload.stamp);
    payload
        .verify_replay(replay_filter, sender)
        .ok_or(DatagramRejection::Replay { seq, stamp })
}
