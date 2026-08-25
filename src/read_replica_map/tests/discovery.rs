// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use super::super::*;
use super::{ephemeral_config, wait_until};

/// A discovery source returning a fixed, swappable response — the read-replica analogue of
/// `replicated_map/tests/discovery.rs`'s `FakeDiscovery`, minus the presence bookkeeping a read
/// replica has no use for (no membership, no GC gate to protect).
#[derive(Clone)]
enum FakeDiscoveryResp {
    /// A successful resolution returning this address list.
    Present(Vec<IpAddr>),
    /// A transient failure (e.g. a DNS blip).
    Blip,
}

#[derive(Clone)]
struct FakeDiscovery {
    resp: Arc<parking_lot::RwLock<FakeDiscoveryResp>>,
}

impl FakeDiscovery {
    fn new(addrs: Vec<IpAddr>) -> Self {
        FakeDiscovery {
            resp: Arc::new(parking_lot::RwLock::new(FakeDiscoveryResp::Present(addrs))),
        }
    }

    fn set(&self, resp: FakeDiscoveryResp) {
        *self.resp.write() = resp;
    }
}

impl crate::discovery::Discovery for FakeDiscovery {
    fn discover(&self) -> crate::discovery::DiscoverFuture<'_> {
        let resp = self.resp.read().clone();
        Box::pin(async move {
            match resp {
                FakeDiscoveryResp::Present(addrs) => Ok(addrs),
                FakeDiscoveryResp::Blip => Err(Box::new(std::io::Error::other("blip")) as _),
            }
        })
    }

    fn kind(&self) -> crate::discovery::DiscoveryKind {
        // Deliberately `Authoritative`: #30's point is that `ReadReplicaMap` treats every kind
        // identically (no decommissioning either way), unlike `ReplicatedMap`.
        crate::discovery::DiscoveryKind::Authoritative
    }
}

/// #30: `with_discovery`/`with_discovery_interval` are plain builders, and any [`Discovery`]
/// implementation is accepted regardless of `kind()` — there is no
/// [`with_discovery`](crate::ReplicatedMap::with_discovery)-style panic guard here, because a
/// read replica holds no membership a wrongly-decommissioned entry could corrupt.
#[test]
fn with_discovery_and_interval_are_builders() {
    let network = crate::transport::InMemoryNetwork::new();
    let transport = Arc::new(network.bind("127.0.9.70:1".parse().unwrap()));
    let read_replica =
        ReadReplicaMap::<i32, String>::new_with_transport(ephemeral_config(), transport)
            .with_discovery(Arc::new(FakeDiscovery::new(vec![])))
            .with_discovery_interval(Duration::from_millis(42));
    assert!(read_replica.discovery.is_some());
    assert_eq!(read_replica.discovery_interval, Duration::from_millis(42));
}

/// #30: `with_dns_discovery` is `with_discovery` plus construction of a `DnsDiscovery` — it must
/// actually set the field, not silently no-op.
#[test]
fn with_dns_discovery_sets_a_discovery_source() {
    let network = crate::transport::InMemoryNetwork::new();
    let transport = Arc::new(network.bind("127.0.9.71:1".parse().unwrap()));
    let read_replica =
        ReadReplicaMap::<i32, String>::new_with_transport(ephemeral_config(), transport)
            .with_dns_discovery("my-service.default.svc.cluster.local", 8080);
    assert!(read_replica.discovery.is_some());
}

/// #30 (the "major gap" row): with no discovery source configured, `discover_periodically` must
/// return promptly rather than looping forever — `run()`'s `tokio::join!` would otherwise hang
/// waiting on it for every replica that never calls `with_discovery`.
#[tokio::test]
async fn discover_periodically_is_a_noop_without_a_configured_source() {
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        read_replica.discover_periodically(),
    )
    .await;
    assert!(
        result.is_ok(),
        "discover_periodically must return immediately with no source configured"
    );
}

/// #30 (the "major gap" row): a resolved address is seeded into the peer set exactly like a peer
/// discovered by answering a probe, closing "no discovery ⇒ not deployable on Kubernetes".
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_seeds_resolved_addresses_as_peers() {
    let discovered: IpAddr = "127.0.9.50".parse().unwrap();
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(FakeDiscovery::new(vec![discovered])))
        .with_discovery_interval(Duration::from_millis(10));

    assert!(!read_replica.get_peers().contains(&discovered));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    assert!(
        wait_until(|| read_replica.get_peers().contains(&discovered)).await,
        "a resolved address was never seeded as a peer"
    );
    handle.abort();
}

/// #30: a read replica must never seed its own address as a peer, mirroring
/// [`ReplicatedMap::discover_periodically`](crate::ReplicatedMap)'s identical self-exclusion.
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_never_seeds_its_own_address() {
    let port = crate::replica::tests::next_ephemeral_test_port();
    let own_addr: IpAddr = "127.0.9.60".parse().unwrap();
    let other: IpAddr = "127.0.9.61".parse().unwrap();
    let read_replica = ReadReplicaMap::<i32, String>::new(
        crate::replicated_map::Config::default()
            .with_port(port)
            .with_listen_addr(own_addr)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed")
    .with_discovery(Arc::new(FakeDiscovery::new(vec![own_addr, other])))
    .with_discovery_interval(Duration::from_millis(10));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    assert!(
        wait_until(|| read_replica.get_peers().contains(&other)).await,
        "the other discovered address was never seeded"
    );
    assert!(
        !read_replica.get_peers().contains(&own_addr),
        "a read replica must never seed its own address as a peer"
    );
    handle.abort();
}

/// #30: a failing discovery round (e.g. a DNS blip) must seed nothing and must not stop the
/// loop — a later successful round still seeds normally, proving the failure is transient
/// rather than fatal.
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_survives_a_failed_round_and_recovers() {
    let discovered: IpAddr = "127.0.9.65".parse().unwrap();
    let fake = FakeDiscovery::new(vec![]);
    fake.set(FakeDiscoveryResp::Blip);
    let read_replica = ReadReplicaMap::<i32, String>::new(ephemeral_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(10));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        read_replica.get_peers().is_empty(),
        "a failing discovery round must never seed a peer"
    );

    fake.set(FakeDiscoveryResp::Present(vec![discovered]));
    assert!(
        wait_until(|| read_replica.get_peers().contains(&discovered)).await,
        "discovery must recover and seed once the source starts succeeding again"
    );
    handle.abort();
}
