// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use crate::read_replica_set::ReadReplicaSet;
use crate::replicated_map::{Config, MAX_NETS};
use rsos::Fingerprint;

async fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}

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
        repair_interval: Duration::from_millis(150),
        bulk_send_rate: Some(32 * 1024 * 1024),
        recv_buffer_size: Some(8 * 1024 * 1024),
        send_buffer_size: Some(8 * 1024 * 1024),
        freshness_window: gossip::replay::FRESHNESS_WINDOW_DEFAULT,
        max_peers: 1024,
        max_concurrent_bulk_dumps: 4,
        max_concurrent_broadcasts: 1024,
        snapshot_interval: Some(Duration::from_secs(5)),
        snapshot_change_threshold: 1,
        max_clock_drift: crate::clock::MAX_CLOCK_DRIFT,
        coalesce_window: Duration::ZERO,
        max_value_size: None,
    }
}

/// #377: a freshly constructed `ReadReplicaSet` holds no member, and `contains`/`len`/
/// `is_empty`/`keys` agree on that.
#[tokio::test]
async fn fresh_replica_has_no_members() {
    let replica = ReadReplicaSet::<i32>::new(ephemeral_config())
        .await
        .unwrap();

    assert!(replica.is_empty());
    assert_eq!(replica.len(), 0);
    assert!(!replica.contains(&1));
    assert!(replica.keys().is_empty());
}

/// #294: after converging with a real dated peer, `value_fingerprint` must actually reflect
/// the received members (not a default/zero `Fingerprint`), and the deprecated `fingerprint`
/// alias must forward to it — a mutant that no-ops either would pass any test that never
/// compares its result to the real, non-default value.
#[tokio::test(flavor = "multi_thread")]
#[allow(deprecated)]
async fn value_fingerprint_and_its_deprecated_alias_reflect_converged_content() {
    use crate::replicated_set::ReplicatedSet;
    use tokio_util::sync::CancellationToken;

    let port = crate::replica::tests::next_ephemeral_test_port();
    let net: ipnet::IpNet = "127.0.6.0/24".parse().unwrap();
    let dated_addr: std::net::IpAddr = "127.0.6.10".parse().unwrap();
    let replica_addr: std::net::IpAddr = "127.0.6.11".parse().unwrap();

    let dated = ReplicatedSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(dated_addr)
            .with_net(net)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed");
    assert!(!dated.insert(7), "key 7 must be newly inserted");

    let replica = ReadReplicaSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(replica_addr)
            .with_net(net)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed")
    .with_seed(dated_addr);

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let replica_task = tokio::spawn(replica.clone().run());

    let mut converged = false;
    for _ in 0..300 {
        if replica.contains(&7) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dated_task.abort();
    replica_task.abort();
    assert!(converged, "replica never observed the dated peer's member");

    let value_fingerprint = replica.value_fingerprint(..);
    assert_ne!(
        value_fingerprint,
        Fingerprint::default(),
        "a converged, non-empty set must not fingerprint as empty"
    );
    assert_eq!(replica.fingerprint(..), value_fingerprint);
}

/// #294: `start_reconciliation` (the public wrapper) must actually send a value-only
/// comparison round to the seeded peer — a mutant that no-ops its body would leave the peer's
/// socket silent forever, which this test's `recv_from` would then time out on. No `run()`
/// loop is spawned on either side, so nothing but this call can produce the datagram.
#[tokio::test]
async fn start_reconciliation_wrapper_actually_transmits() {
    let port = crate::replica::tests::next_ephemeral_test_port();
    let net: ipnet::IpNet = "127.0.6.0/24".parse().unwrap();
    let replica_addr: std::net::IpAddr = "127.0.6.20".parse().unwrap();
    let peer_addr: std::net::IpAddr = "127.0.6.21".parse().unwrap();

    let peer_socket = tokio::net::UdpSocket::bind((peer_addr, port))
        .await
        .expect("peer bind failed");

    let replica = ReadReplicaSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(replica_addr)
            .with_net(net)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed")
    .with_seed(peer_addr);

    replica.start_reconciliation().await;

    let mut buf = [0u8; 65536];
    let (size, from) =
        tokio::time::timeout(Duration::from_secs(5), peer_socket.recv_from(&mut buf))
            .await
            .expect("start_reconciliation never sent anything to the seeded peer")
            .expect("recv_from failed");
    assert!(size > 0, "the datagram sent to the peer was empty");
    assert_eq!(from.ip(), replica_addr);
}

/// #30: `with_discovery`/`with_discovery_interval` actually forward into the wrapped
/// `ReadReplicaMap`'s `run()` loop — a replica configured with discovery alone (no
/// `with_seed`) still converges once discovery resolves the dated peer.
#[tokio::test(flavor = "multi_thread")]
async fn with_discovery_converges_without_with_seed() {
    use crate::discovery::{DiscoverFuture, Discovery, DiscoveryKind};
    use crate::replicated_set::ReplicatedSet;
    use std::net::IpAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone)]
    struct FixedDiscovery(IpAddr);
    impl Discovery for FixedDiscovery {
        fn discover(&self) -> DiscoverFuture<'_> {
            let addr = self.0;
            Box::pin(async move { Ok(vec![addr]) })
        }
        fn kind(&self) -> DiscoveryKind {
            DiscoveryKind::Authoritative
        }
    }

    let port = crate::replica::tests::next_ephemeral_test_port();
    let net: ipnet::IpNet = "127.0.7.0/24".parse().unwrap();
    let dated_addr: IpAddr = "127.0.7.10".parse().unwrap();
    let replica_addr: IpAddr = "127.0.7.11".parse().unwrap();

    let dated = ReplicatedSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(dated_addr)
            .with_net(net)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed");
    assert!(!dated.insert(9), "key 9 must be newly inserted");

    let replica = ReadReplicaSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(replica_addr)
            .with_net(net)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed")
    .with_discovery(Arc::new(FixedDiscovery(dated_addr)))
    .with_discovery_interval(Duration::from_millis(15));

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let replica_task = tokio::spawn(replica.clone().run());

    let mut converged = false;
    for _ in 0..300 {
        if replica.contains(&9) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dated_task.abort();
    replica_task.abort();
    assert!(
        converged,
        "replica never converged via discovery alone (no with_seed)"
    );
}

/// #30: `with_dns_discovery` must actually wire a `DnsDiscovery` source into the replica, not
/// silently no-op — resolving "localhost" finds a dated peer bound on loopback, with no
/// `with_seed` needed.
#[tokio::test(flavor = "multi_thread")]
async fn with_dns_discovery_converges_via_localhost_resolution() {
    use crate::replicated_set::ReplicatedSet;
    use tokio_util::sync::CancellationToken;

    let port = crate::replica::tests::next_ephemeral_test_port();
    let dated_addr: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let replica_addr: std::net::IpAddr = "127.0.7.40".parse().unwrap();

    let dated = ReplicatedSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(dated_addr)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed");
    assert!(!dated.insert(11), "key 11 must be newly inserted");

    let replica = ReadReplicaSet::<i32>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(replica_addr)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed")
    .with_dns_discovery("localhost", port)
    .with_discovery_interval(Duration::from_millis(15));

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let replica_task = tokio::spawn(replica.clone().run());

    let mut converged = false;
    for _ in 0..300 {
        if replica.contains(&11) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dated_task.abort();
    replica_task.abort();
    assert!(
        converged,
        "replica never converged via DNS discovery resolving localhost"
    );
}

/// #8: `local_addr` forwards to the wrapped `ReadReplicaMap` and reports the transport's real
/// bound address, matching what was configured — mirrors #292's own `ReplicatedMap` test and
/// `ReadReplicaMap`'s own `local_addr_matches_the_configured_bind_address`.
#[tokio::test]
async fn local_addr_matches_the_configured_bind_address() {
    let config = ephemeral_config();
    let expected = SocketAddr::new(config.listen_addr, config.port);
    let replica = ReadReplicaSet::<i32>::new(config)
        .await
        .expect("bind failed");
    assert_eq!(
        replica
            .local_addr()
            .expect("transport must report its address"),
        expected
    );
}

/// #8: `sync_state` forwards to the wrapped `ReadReplicaMap` — it starts with no rounds
/// initiated and advances once `run()` is actually driving the replica, rather than returning a
/// default.
#[tokio::test(flavor = "multi_thread")]
async fn sync_state_advances_as_the_replica_runs() {
    let replica = ReadReplicaSet::<i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let initial = replica.sync_state();
    assert_eq!(initial.rounds, 0);
    assert!(initial.last_round_at.is_none());

    let task = tokio::spawn(replica.clone().run());
    assert!(
        wait_until(|| {
            let state = replica.sync_state();
            state.rounds > 0 && state.last_round_at.is_some()
        })
        .await,
        "sync_state never observed an initiated reconciliation round"
    );
    task.abort();
}

/// #8: `seed_peer` forwards to the wrapped `ReadReplicaMap` — the `&self` counterpart of
/// `with_seed`, it registers a brand-new peer so it is immediately visible via `peers`.
#[tokio::test]
async fn seed_peer_registers_a_peer_visible_via_peers() {
    let replica = ReadReplicaSet::<i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let peer: IpAddr = "127.0.0.213".parse().unwrap();

    assert!(
        !replica.peers().contains(&peer),
        "test setup: peer must start unknown"
    );
    replica.seed_peer(peer);
    assert!(
        replica.peers().contains(&peer),
        "seed_peer must register a brand-new peer, visible via peers()"
    );
}

/// #8: `set_reconcile_interval` forwards to the wrapped `ReadReplicaMap` and actually retunes
/// `run`'s idle re-initiation cadence at runtime — a 3600s configured interval that is never
/// retuned would leave `rounds` at `1` (the unconditional round fired at `run()` entry) for the
/// entire test window.
#[tokio::test(flavor = "multi_thread")]
async fn set_reconcile_interval_actually_retunes_the_idle_timeout() {
    let replica = ReadReplicaSet::<i32>::new(
        ephemeral_config().with_reconcile_interval(Duration::from_secs(3600)),
    )
    .await
    .expect("bind failed");
    replica.set_reconcile_interval(Duration::from_millis(20));

    let task = tokio::spawn(replica.clone().run());
    assert!(
        wait_until(|| replica.sync_state().rounds >= 5).await,
        "retuning reconcile_interval must speed up the idle re-init cadence, not wait out the \
         original 3600s interval"
    );
    task.abort();
}
