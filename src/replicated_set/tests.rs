// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::replicated_map::{Config, MAX_NETS};
use crate::transport::InMemoryNetwork;
use crate::{ReplicatedMap, ReplicatedSet};

fn ephemeral_config() -> Config {
    Config {
        port: crate::replica::tests::next_ephemeral_test_port(),
        listen_addr: "127.0.0.1".parse().unwrap(),
        nets: [None; MAX_NETS],
        remote_interval: 6,
        remote_fanout: 2,
        cluster_key: None,
        insecure_no_key: true,
        node_id: None,
        encrypt: false,
        reconcile_interval: Duration::from_secs(1),
        bulk_send_rate: Some(32 * 1024 * 1024),
        recv_buffer_size: Some(8 * 1024 * 1024),
        send_buffer_size: Some(8 * 1024 * 1024),
        freshness_window: gossip::replay::FRESHNESS_WINDOW_DEFAULT,
        max_peers: 1024,
        max_concurrent_bulk_dumps: 4,
        snapshot_interval: Duration::from_secs(5),
        max_clock_drift: crate::clock::MAX_CLOCK_DRIFT,
        coalesce_window: Duration::ZERO,
    }
}

/// #377: `insert`/`remove` report prior presence, `contains` reports current presence, and
/// bulk/`len`/`is_empty` track the same membership.
#[tokio::test]
async fn insert_remove_contains_and_bulk_agree_on_membership() {
    let set = ReplicatedSet::<i32>::new(ephemeral_config()).await.unwrap();

    assert!(set.is_empty());
    assert!(!set.contains(&1));
    assert!(!set.insert(1)); // wasn't present
    assert!(set.contains(&1));
    assert!(set.insert(1)); // already present, idempotent
    assert_eq!(set.len(), 1);

    set.insert_bulk(&[2, 3]);
    assert_eq!(set.len(), 3);
    assert!(set.contains(&2) && set.contains(&3));

    assert!(set.remove(&1)); // was present
    assert!(!set.contains(&1));
    assert!(!set.remove(&1)); // already gone

    set.remove_bulk(&[2, 3]);
    assert!(set.is_empty());
}

/// `ReplicatedSet::set_nets` is a thin delegate to `ReplicatedMap::set_nets` — assert the
/// delegation actually happens (the `MAX_NETS` cap is enforced through it), not just that
/// calling it doesn't panic.
#[tokio::test]
async fn set_nets_enforces_max_nets_at_runtime() {
    let set = ReplicatedSet::<i32>::new(ephemeral_config()).await.unwrap();

    let within_cap: Vec<_> = (0..MAX_NETS)
        .map(|i| format!("127.0.0.0/{}", 8 + (i % 24)).parse().unwrap())
        .collect();
    set.set_nets(&within_cap)
        .expect("exactly MAX_NETS networks should be accepted");

    let over_cap: Vec<_> = (0..=MAX_NETS)
        .map(|i| format!("127.0.0.0/{}", 8 + (i % 24)).parse().unwrap())
        .collect();
    assert_eq!(
        set.set_nets(&over_cap),
        Err(crate::replicated_map::ConfigError::TooManyNets),
        "MAX_NETS + 1 networks should be rejected"
    );
}

/// `ReplicatedSet::set_coalesce_window` is a thin delegate to
/// `ReplicatedMap::set_coalesce_window` — assert the delegation actually happens, not just
/// that calling it doesn't panic (same rationale as `set_nets_enforces_max_nets_at_runtime`
/// above).
#[tokio::test]
async fn set_coalesce_window_actually_retunes_the_engine() {
    let set = ReplicatedSet::<i32>::new(ephemeral_config()).await.unwrap();
    assert_eq!(set.0.coalesce_window(), Duration::ZERO);

    set.set_coalesce_window(Duration::from_millis(123));
    assert_eq!(set.0.coalesce_window(), Duration::from_millis(123));
}

async fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}

/// #481: `local_addr` forwards to the wrapped map and reports the transport's real bound
/// address, matching what was configured — mirrors #292's own `ReplicatedMap` test.
#[tokio::test]
async fn local_addr_matches_the_configured_bind_address() {
    let config = ephemeral_config();
    let expected = SocketAddr::new(config.listen_addr, config.port);
    let set = ReplicatedSet::<i32>::new(config)
        .await
        .expect("bind failed");
    assert_eq!(
        set.local_addr().expect("transport must report its address"),
        expected
    );
}

/// #481: `sync_state` forwards to the wrapped map — it starts with no rounds completed and
/// advances as the engine actually runs, rather than returning a default.
#[tokio::test(flavor = "multi_thread")]
async fn sync_state_advances_as_the_engine_runs() {
    let set = ReplicatedSet::<i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let initial = set.sync_state();
    assert_eq!(initial.rounds, 0);
    assert!(initial.last_round_at.is_none());

    let task = tokio::spawn(set.clone().run(CancellationToken::new()));
    assert!(
        wait_until(|| set.sync_state().rounds > 0 && set.sync_state().last_round_at.is_some())
            .await,
        "sync_state never observed a completed reconciliation round"
    );
    task.abort();
}

/// #481: `peers`/`members` forward to the wrapped map and reflect a real converged pair of
/// sets — each only contains the other node's address once a genuine datagram has been
/// exchanged, so neither is a fixed literal nor an empty default.
#[tokio::test(flavor = "multi_thread")]
async fn peers_and_members_reflect_a_converged_pair() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.11.1".parse().unwrap();
    let b_ip: IpAddr = "127.0.11.2".parse().unwrap();
    let cfg = |ip: IpAddr| {
        ephemeral_config()
            .with_listen_addr(ip)
            .with_port(port)
            .with_reconcile_interval(Duration::from_millis(20))
    };
    let a = ReplicatedSet::<i32>(ReplicatedMap::new_with_transport(
        cfg(a_ip),
        Arc::new(net.bind(SocketAddr::new(a_ip, port))),
    ))
    .with_seed(b_ip);
    let b = ReplicatedSet::<i32>(ReplicatedMap::new_with_transport(
        cfg(b_ip),
        Arc::new(net.bind(SocketAddr::new(b_ip, port))),
    ))
    .with_seed(a_ip);

    // `with_seed` above already registers the peer for gossip routing; membership is earned only
    // through a real accepted datagram, so it must still be empty at this point.
    assert!(a.members().is_empty());

    let ta = tokio::spawn(a.clone().run(CancellationToken::new()));
    let tb = tokio::spawn(b.clone().run(CancellationToken::new()));

    assert!(
        wait_until(|| a.peers().contains(&b_ip) && a.members().contains(&b_ip)).await,
        "A never registered B as a peer/member"
    );
    assert!(
        wait_until(|| b.peers().contains(&a_ip) && b.members().contains(&a_ip)).await,
        "B never registered A as a peer/member"
    );

    ta.abort();
    tb.abort();
}
