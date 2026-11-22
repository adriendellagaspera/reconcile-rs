// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::{Duration, Instant};

use super::{isolated_read_replica, virtual_config};

/// Expired routing entries are removed while recently-heard-from peers remain.
///
/// This checks both sides of the expiry window. It does not pin the exact boundary: the map uses
/// `Instant::now()` directly and has no injectable clock, so a boundary assertion would be timing-sensitive.
#[tokio::test]
async fn peers_drops_expired_entries_but_keeps_fresh_ones() {
    let read_replica = isolated_read_replica::<i32, String>(virtual_config());
    let stale: std::net::IpAddr = "127.0.0.201".parse().unwrap();
    let fresh: std::net::IpAddr = "127.0.0.202".parse().unwrap();

    read_replica
        .peers
        .write()
        .insert(stale, Instant::now() - Duration::from_secs(61));
    read_replica
        .peers
        .write()
        .insert(fresh, Instant::now() - Duration::from_secs(1));

    let remaining = read_replica.peers();
    assert!(
        !remaining.contains(&stale),
        "a peer silent past PEER_EXPIRATION must be dropped"
    );
    assert!(
        remaining.contains(&fresh),
        "a recently-heard-from peer must be kept"
    );
}
