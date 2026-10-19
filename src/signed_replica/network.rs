// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Signed-only, versioned message encoding and physical-address-agnostic I/O.
//! Authentication, returnable-route proof and replay happen before any decode or state mutation.

use super::*;

use gossip::framing::{
    fragment_payload_capacity, max_missing_ranges_for_budget, transfer_id, write_complete,
    write_completion_ack, write_fragment, write_missing_report, ReceiveOutcome, RecoveryControl,
    FRAGMENT_HEADER_LEN,
};
use gossip::peer::routes::SignedRouteReceive;
use serde::{Deserialize, Serialize};

/// The signed runtime's dated protocol. Legacy SocketAddr runtime frames are *not* accepted:
/// both Ed25519 and the inner MAC/wire-version boundary are always required first.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum SignedMessage<K, V> {
    /// Initiate a signed, route-confirmed exchange; never grants causal membership by itself.
    Hello,
    /// Authoritative dated LWW record; a reader never gets to originate this variant.
    Update(K, Entry<Timestamp, V>),
    /// Acknowledgement for an exact locally held tombstone version.
    Ack(K, u64),
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn serialize<K: Serialize, V: Serialize>(message: &SignedMessage<K, V>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    gossip::bincode::encode(message, &mut bytes).map_err(|err| invalid(err.to_string()))?;
    Ok(bytes)
}

mod receive;
mod send;
