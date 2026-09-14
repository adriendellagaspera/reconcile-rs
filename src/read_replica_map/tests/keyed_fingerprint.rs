// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `ReadReplicaMap::build`'s `Config::cluster_key` -> `rsos::LiftKey` seam -- the same property
//! `src/replica/tests/keyed_fingerprint.rs` covers for `Replica`, exercised here because
//! `ReadReplicaMap` builds its own tree independently (its own `build`, not a shared helper).
//! `integrate` (not a real network round) is enough: this is about the seam wiring a configured
//! key into `FingerprintTreeMap::with_lift_key`, not about reconciliation itself.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use gossip::auth::{Authenticator, ClusterKey};
use gossip::replay::{Seq, Stamp};

use crate::entry::State;
use crate::replicated_map::Config;
use crate::transport::InMemoryNetwork;
use crate::ReadReplicaMap;

fn read_replica_with_key(
    net: &InMemoryNetwork,
    ip: IpAddr,
    cluster_key: Option<ClusterKey>,
) -> ReadReplicaMap<u32, u32> {
    let port = 4900u16;
    let mut config = Config::default().with_listen_addr(ip).with_port(port);
    config = match cluster_key {
        Some(key) => config.with_cluster_key(key),
        None => config.with_insecure_no_key(),
    };
    ReadReplicaMap::new_with_transport(config, Arc::new(net.bind(SocketAddr::new(ip, port))))
}

fn load(replica: &ReadReplicaMap<u32, u32>, entries: &[(u32, u32)]) {
    replica.integrate(
        entries
            .iter()
            .map(|&(k, v)| (k, State::Present(v)))
            .collect(),
    );
}

#[test]
fn same_cluster_key_yields_matching_fingerprints() {
    let net = InMemoryNetwork::new();
    let key = ClusterKey::new([1; 32]);
    let a = read_replica_with_key(&net, "127.0.11.1".parse().unwrap(), Some(key.clone()));
    let b = read_replica_with_key(&net, "127.0.11.2".parse().unwrap(), Some(key));
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&a, &entries);
    load(&b, &entries);
    assert_eq!(a.value_fingerprint(..), b.value_fingerprint(..));
}

#[test]
fn a_cluster_key_actually_changes_the_fingerprint_the_unkeyed_lift_would_produce() {
    let net = InMemoryNetwork::new();
    let keyed = read_replica_with_key(
        &net,
        "127.0.11.3".parse().unwrap(),
        Some(ClusterKey::new([2; 32])),
    );
    let unkeyed = read_replica_with_key(&net, "127.0.11.4".parse().unwrap(), None);
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&keyed, &entries);
    load(&unkeyed, &entries);
    // Guards against the seam silently no-oping (e.g. the derived key never reaching
    // `FingerprintTreeMap::with_lift_key`, leaving every read replica unkeyed regardless of config).
    assert_ne!(keyed.value_fingerprint(..), unkeyed.value_fingerprint(..));
}

#[test]
fn rotation_fallback_is_receive_only_on_a_read_replica() {
    let net = InMemoryNetwork::new();
    let port = 4900u16;
    let rotating_ip: IpAddr = "127.0.11.5".parse().unwrap();
    let primary_ip: IpAddr = "127.0.11.6".parse().unwrap();
    let primary = ClusterKey::new([3; 32]);
    let fallback = ClusterKey::new([4; 32]);

    let rotating = ReadReplicaMap::<u32, u32>::new_with_transport(
        Config::default()
            .with_listen_addr(rotating_ip)
            .with_port(port)
            .with_cluster_key_rotation(primary.clone(), fallback.clone()),
        Arc::new(net.bind(SocketAddr::new(rotating_ip, port))),
    );

    // The fallback really reaches ReadReplicaMap's independently-built authenticator.
    let fallback_sender = Authenticator::new(Some(fallback), false).unwrap();
    let datagram = fallback_sender.seal(Seq::FIRST, Stamp::NONE, b"rotation-probe");
    assert!(rotating.authenticator.open(&datagram).is_some());

    // It does not influence the value-only fingerprint tree: the primary alone defines it.
    let primary_only = read_replica_with_key(&net, primary_ip, Some(primary));
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&rotating, &entries);
    load(&primary_only, &entries);
    assert_eq!(
        rotating.value_fingerprint(..),
        primary_only.value_fingerprint(..)
    );
}
