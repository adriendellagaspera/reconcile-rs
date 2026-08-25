// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `Replica::build`'s `Config::cluster_key` -> `rsos::LiftKey` seam (issue #19): a `Replica`
//! constructed with a cluster key must actually key its `map`/`projection` trees, matching what
//! another node with the identical key would compute, and never a node with a different one --
//! the property `rbsr::protocol_round`'s divergence detection depends on end to end.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use gossip::auth::ClusterKey;

use crate::clock::{ManualClock, NodeId};
use crate::entry::Entry;
use crate::replica::Replica;
use crate::replicated_map::Config;
use crate::transport::InMemoryNetwork;

/// A `Replica` bound to an in-memory transport (no real socket), with `cluster_key` as given. Not
/// run: this only exercises construction and local inserts, never the network loop.
fn replica_with_key(
    net: &InMemoryNetwork,
    ip: IpAddr,
    cluster_key: Option<ClusterKey>,
) -> Replica<u32, u32> {
    let port = 4800u16;
    let mut config = Config::default().with_listen_addr(ip).with_port(port);
    config = match cluster_key {
        Some(key) => config.with_cluster_key(key),
        None => config.with_insecure_no_key(),
    };
    Replica::new_with_transport(
        config,
        Arc::new(net.bind(SocketAddr::new(ip, port))),
        Arc::new(ManualClock::new(NodeId::new(1))),
    )
}

/// Load identical entries into `replica`'s map at the same logical instant on both call sites, so
/// only the lift key -- not the timestamp -- can move the resulting fingerprint.
fn load(replica: &Replica<u32, u32>, entries: &[(u32, u32)]) {
    for (k, v) in entries {
        replica.just_insert(*k, Entry::present(replica.clock_now(), *v));
    }
}

#[test]
fn same_cluster_key_yields_matching_fingerprints() {
    let net = InMemoryNetwork::new();
    let key = ClusterKey::new([1; 32]);
    let a = replica_with_key(&net, "127.0.10.1".parse().unwrap(), Some(key.clone()));
    let b = replica_with_key(&net, "127.0.10.2".parse().unwrap(), Some(key));
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&a, &entries);
    load(&b, &entries);
    // Two nodes deriving the identical BLAKE3 subkey from the identical cluster key must compute
    // the identical fingerprint for identical content -- the wiring this issue adds must not turn
    // two honestly-configured peers into permanent strangers.
    assert_eq!(
        a.map.load_full().aggregate(..).fingerprint(),
        b.map.load_full().aggregate(..).fingerprint()
    );
}

#[test]
fn different_cluster_keys_yield_different_fingerprints() {
    let net = InMemoryNetwork::new();
    let a = replica_with_key(
        &net,
        "127.0.10.3".parse().unwrap(),
        Some(ClusterKey::new([2; 32])),
    );
    let b = replica_with_key(
        &net,
        "127.0.10.4".parse().unwrap(),
        Some(ClusterKey::new([3; 32])),
    );
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&a, &entries);
    load(&b, &entries);
    assert_ne!(
        a.map.load_full().aggregate(..).fingerprint(),
        b.map.load_full().aggregate(..).fingerprint()
    );
}

#[test]
fn a_cluster_key_actually_changes_the_fingerprint_the_unkeyed_lift_would_produce() {
    let net = InMemoryNetwork::new();
    let keyed = replica_with_key(
        &net,
        "127.0.10.5".parse().unwrap(),
        Some(ClusterKey::new([4; 32])),
    );
    let unkeyed = replica_with_key(&net, "127.0.10.6".parse().unwrap(), None);
    let entries = [(1, 10), (2, 20), (3, 30)];
    load(&keyed, &entries);
    load(&unkeyed, &entries);
    // Guards against the seam silently no-oping (e.g. the derived key never reaching
    // `FingerprintTreeMap::with_lift_key`, leaving every replica unkeyed regardless of config).
    assert_ne!(
        keyed.map.load_full().aggregate(..).fingerprint(),
        unkeyed.map.load_full().aggregate(..).fingerprint()
    );
}
