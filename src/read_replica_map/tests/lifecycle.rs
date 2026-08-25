// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #30: the introspection/lifecycle accessors closed by that issue —
//! `local_addr`/`sync_state`/`seed_peer`/`set_reconcile_interval` — mirroring
//! `replicated_map/tests/lifecycle.rs`'s equivalent coverage for `ReplicatedMap`.

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use super::super::*;
use super::{ephemeral_config, wait_until};

/// `local_addr` reports the transport's real bound address, matching what was configured —
/// mirrors `ReplicatedMap`'s own test of the same name.
#[tokio::test]
async fn local_addr_matches_the_configured_bind_address() {
    let config = ephemeral_config();
    let expected = SocketAddr::new(config.listen_addr, config.port);
    let read_replica = ReadReplicaMap::<i32, i32>::new(config)
        .await
        .expect("bind failed");
    assert_eq!(
        read_replica
            .local_addr()
            .expect("transport must report its address"),
        expected
    );
}

/// `sync_state` starts with no rounds initiated and advances once `run()` is actually driving the
/// replica — a mutant that no-ops the round counter or the `last_round_at` write would leave this
/// at its initial `0`/`None` forever.
#[tokio::test(flavor = "multi_thread")]
async fn sync_state_advances_as_the_replica_runs() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let initial = read_replica.sync_state();
    assert_eq!(initial.rounds, 0);
    assert!(initial.last_round_at.is_none());

    let task = tokio::spawn(read_replica.clone().run());
    assert!(
        wait_until(|| {
            let state = read_replica.sync_state();
            state.rounds > 0 && state.last_round_at.is_some()
        })
        .await,
        "sync_state never observed an initiated reconciliation round"
    );
    task.abort();
}

/// `seed_peer` is the `&self` counterpart of `with_seed`: it registers a brand-new peer, and it
/// re-arms an already-known one's expiration window rather than leaving a stale entry to be
/// dropped by `peers`'s `PEER_EXPIRATION` filter.
#[tokio::test]
async fn seed_peer_registers_and_refreshes_a_peer() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let new_peer: IpAddr = "127.0.0.211".parse().unwrap();
    let stale_peer: IpAddr = "127.0.0.212".parse().unwrap();

    read_replica.seed_peer(new_peer);
    assert!(
        read_replica.peers().contains(&new_peer),
        "seed_peer must register a brand-new peer"
    );

    read_replica
        .peers
        .write()
        .insert(stale_peer, Instant::now() - Duration::from_secs(61));
    assert!(
        !read_replica.peers().contains(&stale_peer),
        "test setup: peer must start past PEER_EXPIRATION"
    );
    read_replica.seed_peer(stale_peer);
    assert!(
        read_replica.peers().contains(&stale_peer),
        "seed_peer must re-arm an already-known peer's expiration, not just insert new ones"
    );
}

/// `set_reconcile_interval` must actually retune `run`'s idle re-initiation cadence at runtime,
/// mirroring `ReplicatedMap::set_reconcile_interval`'s own test: a 3600s configured interval that
/// is never retuned would leave `rounds` at `1` (the unconditional round fired at `run()` entry)
/// for the entire test window.
#[tokio::test(flavor = "multi_thread")]
async fn set_reconcile_interval_actually_retunes_the_idle_timeout() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(
        ephemeral_config().with_reconcile_interval(Duration::from_secs(3600)),
    )
    .await
    .expect("bind failed");
    read_replica.set_reconcile_interval(Duration::from_millis(20));

    let task = tokio::spawn(read_replica.clone().run());
    assert!(
        wait_until(|| read_replica.sync_state().rounds >= 5).await,
        "retuning reconcile_interval must speed up the idle re-init cadence, not wait out the \
         original 3600s interval"
    );
    task.abort();
}
