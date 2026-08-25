// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::{Duration, Instant};

use super::super::*;
use super::ephemeral_config;
use ipnet::IpNet;

/// `set_net` retunes what `net` reports, for every clone.
#[tokio::test]
async fn set_net_retunes_what_net_reports() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let original = read_replica.net();
    let retuned: IpNet = "10.77.0.0/16".parse().unwrap();
    assert_ne!(original, retuned, "test needs a genuinely different net");

    read_replica.set_net(retuned);
    assert_eq!(read_replica.net(), retuned);
}

/// `get_peers` drops an entry once it has been silent well past `PEER_EXPIRATION`, but keeps a
/// peer heard from well within the window.
///
/// Not a boundary test: pinning the exact instant `elapsed() == PEER_EXPIRATION` would require
/// either a mockable clock (this map uses real `Instant::now()`, no injection seam) or a
/// real-time race between "compute the target `Instant`" and "the `retain` closure evaluates
/// it" — exactly the flakiness `.claude/rules/tests.md` rules out. See `.cargo/mutants.toml`'s
/// `membership.rs:33:53` entry for the boundary-operator mutant this can't distinguish.
#[tokio::test]
async fn get_peers_drops_expired_entries_but_keeps_fresh_ones() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
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

    let remaining = read_replica.get_peers();
    assert!(
        !remaining.contains(&stale),
        "a peer silent past PEER_EXPIRATION must be dropped"
    );
    assert!(
        remaining.contains(&fresh),
        "a recently-heard-from peer must be kept"
    );
}
