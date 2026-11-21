// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use super::super::*;
use super::wait_until;
use crate::transport::InMemoryNetwork;

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
        // Deliberately `Authoritative`: 's point is that `ReadReplicaMap` treats every kind
        // identically (no decommissioning either way), unlike `ReplicatedMap`.
        crate::discovery::DiscoveryKind::Authoritative
    }
}

fn discovery_config() -> crate::replicated_map::Config {
    crate::replicated_map::Config::default()
        .with_port(5000)
        .with_insecure_no_key()
}

fn read_discovery_replica(config: crate::replicated_map::Config) -> ReadReplicaMap<i32, String> {
    let network = InMemoryNetwork::new();
    let addr = SocketAddr::new(config.listen_addr, config.port);
    ReadReplicaMap::new_with_transport(config, Arc::new(network.bind(addr)))
        .expect("valid discovery test config")
}

/// Without a configured source, `discover_periodically` returns promptly so `run` does not
/// wait forever for a discovery task that has no work to perform.
#[tokio::test]
async fn discover_periodically_is_a_noop_without_a_configured_source() {
    let read_replica = read_discovery_replica(discovery_config());
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

/// A successful discovery round adds resolved remote addresses to the peer set.
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_seeds_resolved_addresses_as_peers() {
    let discovered: IpAddr = "127.0.9.50".parse().unwrap();
    let read_replica = read_discovery_replica(discovery_config())
        .with_discovery(Arc::new(FakeDiscovery::new(vec![discovered])))
        .with_discovery_interval(Duration::from_millis(10));

    assert!(!read_replica.peers().contains(&discovered));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    assert!(
        wait_until(|| read_replica.peers().contains(&discovered)).await,
        "a resolved address was never seeded as a peer"
    );
    handle.abort();
}

/// The local listen address is never added to the peer set, even if the source returns it.
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_never_seeds_its_own_address() {
    let own_addr: IpAddr = "127.0.9.60".parse().unwrap();
    let other: IpAddr = "127.0.9.61".parse().unwrap();
    let read_replica = read_discovery_replica(discovery_config().with_listen_addr(own_addr))
        .with_discovery(Arc::new(FakeDiscovery::new(vec![own_addr, other])))
        .with_discovery_interval(Duration::from_millis(10));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    assert!(
        wait_until(|| read_replica.peers().contains(&other)).await,
        "the other discovered address was never seeded"
    );
    assert!(
        !read_replica.peers().contains(&own_addr),
        "a read replica must never seed its own address as a peer"
    );
    handle.abort();
}

/// A transient discovery failure seeds no peers and does not stop later rounds.
#[tokio::test(flavor = "multi_thread")]
async fn discover_periodically_survives_a_failed_round_and_recovers() {
    let discovered: IpAddr = "127.0.9.65".parse().unwrap();
    let fake = FakeDiscovery::new(vec![]);
    fake.set(FakeDiscoveryResp::Blip);
    let read_replica = read_discovery_replica(discovery_config())
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(10));

    let loop_replica = read_replica.clone();
    let handle = tokio::spawn(async move { loop_replica.discover_periodically().await });

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        read_replica.peers().is_empty(),
        "a failing discovery round must never seed a peer"
    );

    fake.set(FakeDiscoveryResp::Present(vec![discovered]));
    assert!(
        wait_until(|| read_replica.peers().contains(&discovered)).await,
        "discovery must recover and seed once the source starts succeeding again"
    );
    handle.abort();
}
