// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::Duration;

use crate::read_replica_set::ReadReplicaSet;
use crate::replicated_map::{Config, MAX_NETS};
use rsos::Fingerprint;

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
        snapshot_interval: Duration::from_secs(5),
        max_clock_drift: crate::clock::MAX_CLOCK_DRIFT,
        coalesce_window: Duration::ZERO,
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
