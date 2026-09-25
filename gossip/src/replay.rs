// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Replay protection for authenticated datagrams.
//!
//! Each datagram carries an authenticated sequence number and sender timestamp. [`ReplayFilter`]
//! rejects duplicates, stale sequence numbers, and timestamps outside the configured freshness
//! window.
//!
//! Replay state outlives transient membership. A sender restart may reset sequence tracking only
//! when its authenticated timestamp advances beyond the previous maximum; [`SenderCounter`]
//! preserves monotonic sender stamps within a process.
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use chrono::Utc;
use parking_lot::Mutex;

use peer_state::PeerState;

mod bitmap;
mod filter;
mod peer_state;
mod sender;
mod wire;

/// Length of the replay header prepended to the authenticated portion of every datagram.
/// `seq (8 bytes) || stamp (8 bytes)`.
pub const REPLAY_HEADER_LEN: usize = 16;

/// Default freshness window: datagrams whose sender wall-clock stamp deviates from local physical
/// time by more than this value in either direction are rejected.
pub const FRESHNESS_WINDOW_DEFAULT: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Size of the out-of-order acceptance bitmap: a `seq` up to this far behind `max_seq` is accepted
/// as legitimate UDP reordering (one bit per relative sequence number); older is rejected.
const WINDOW_SIZE: u64 = 1024;

/// Read the local physical time as milliseconds since the Unix epoch.
fn phys_now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

/// A per-sender monotonic sequence number carried in the replay header. This module owns its wire
/// encoding and its ordering semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

/// A sender wall-clock stamp (milliseconds since the Unix epoch) carried in the replay header.
/// This module owns its wire encoding and its freshness check.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stamp(u64);

/// Sender-side replay state, one per node. `stamp_floor` keeps minted stamps monotonic within the
/// process — the guarantee the receiver's tail guard relies on, lost on restart (module docs).
#[derive(Debug)]
pub struct SenderCounter {
    seq: AtomicU64,
    stamp_floor: AtomicU64,
}

/// Receiver-side per-peer replay filter.
/// Entries are purged once `now - stamp_at_max > window`, at which point no replayable datagram
/// could clear the freshness check anyway. `enabled` mirrors the owning
/// [`crate::auth::Authenticator`]'s mode, fixed at construction; a disabled filter accepts
/// everything, so no caller decides whether replay-checking applies.
#[derive(Debug)]
pub struct ReplayFilter {
    peers: Mutex<HashMap<IpAddr, PeerState>>,
    freshness_window: Duration,
    enabled: bool,
}

#[cfg(test)]
mod tests;
