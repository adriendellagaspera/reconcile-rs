// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deletion propagation and retained history across partitions, receipt ACKs and decommissioning.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use reconcile::{replicated_map::Config, InMemoryNetwork, ReplicatedMap};

use crate::support::assert_until;

/// Regression test: a value deleted while a replica is partitioned must not be
/// resurrected when that replica returns with the stale value.
#[tokio::test(flavor = "multi_thread")]
async fn deleted_value_is_not_resurrected_by_returning_peer() {
    let port = 5000u16;
    let fabric = InMemoryNetwork::new();
    let net = "127.0.0.1/8".parse().unwrap();
    let addr1 = "127.0.0.70".parse().unwrap();
    let addr2 = "127.0.0.71".parse().unwrap();
    let cfg1 = Config::default()
        .with_port(port)
        .with_listen_addr(addr1)
        .with_net(net)
        .unwrap()
        .with_insecure_no_key();
    let cfg2 = Config::default()
        .with_port(port)
        .with_listen_addr(addr2)
        .with_net(net)
        .unwrap()
        .with_insecure_no_key();

    let store1 = ReplicatedMap::<i32, i32>::new_with_transport(
        cfg1,
        Arc::new(fabric.bind(SocketAddr::new(addr1, port))),
    )
    .expect("valid test config")
    .with_seed(addr2);
    let store2 = ReplicatedMap::<i32, i32>::new_with_transport(
        cfg2,
        Arc::new(fabric.bind(SocketAddr::new(addr2, port))),
    )
    .expect("valid test config")
    .with_seed(addr1);

    let task1 = tokio::spawn(store1.clone().run(CancellationToken::new()));
    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));

    // Both replicas hold key 1 = v, and become members of each other.
    store1.insert(1, 11);
    assert_until!(store2.get(&1).as_deref() == Some(&11));
    store2.insert(2, 22);
    assert_until!(store1.get(&1).as_deref() == Some(&11));
    assert_until!(store1.get(&2).as_deref() == Some(&22));

    // Partition store2 while it still holds the stale value 1 = 11.
    task2.abort();
    assert_eq!(store2.get(&1).as_deref(), Some(&11));

    // Delete key 1 on store1 while store2 is partitioned; the retained tombstone will
    // eventually propagate when store2 returns.
    store1.remove(&1);
    assert!(store1.get(&1).is_none());

    // store2 returns with the stale value and reconciles.
    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));

    // The deletion propagates to store2; crucially, the stale value never resurrects on store1.
    assert_until!(store2.get(&1).is_none());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        store1.get(&1).is_none(),
        "deleted value was resurrected by the returning partitioned peer"
    );
    assert!(
        store2.get(&1).is_none(),
        "deletion did not reach the returning peer"
    );

    task1.abort();
    task2.abort();
}

/// A deletion must converge across a three-node full mesh.
#[tokio::test(flavor = "multi_thread")]
async fn retained_tombstones_converge_in_3_node_cluster_mesh() {
    let port = 5000u16;
    let fabric = InMemoryNetwork::new();
    let net = "127.0.0.1/8".parse().unwrap();
    let addr1 = "127.0.0.110".parse().unwrap();
    let addr2 = "127.0.0.111".parse().unwrap();
    let addr3 = "127.0.0.112".parse().unwrap();

    let mk = |addr| {
        Config::default()
            .with_port(port)
            .with_listen_addr(addr)
            .with_net(net)
            .unwrap()
            .with_reconcile_interval(Duration::from_millis(100))
            .with_insecure_no_key()
    };
    let store1 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr1),
        Arc::new(fabric.bind(SocketAddr::new(addr1, port))),
    )
    .expect("valid test config")
    .with_seed(addr2)
    .with_seed(addr3);
    let store2 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr2),
        Arc::new(fabric.bind(SocketAddr::new(addr2, port))),
    )
    .expect("valid test config")
    .with_seed(addr1)
    .with_seed(addr3);
    let store3 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr3),
        Arc::new(fabric.bind(SocketAddr::new(addr3, port))),
    )
    .expect("valid test config")
    .with_seed(addr1)
    .with_seed(addr2);

    let task1 = tokio::spawn(store1.clone().run(CancellationToken::new()));
    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));
    let task3 = tokio::spawn(store3.clone().run(CancellationToken::new()));

    // Each node writes a distinct live key; once all keys are visible everywhere, every pair has
    // exchanged dated messages, so all three are mutual authoritative members.
    store1.insert(1, 11);
    store2.insert(2, 22);
    store3.insert(3, 33);
    assert_until!(store1.get(&2).as_deref() == Some(&22) && store1.get(&3).as_deref() == Some(&33));
    assert_until!(store2.get(&1).as_deref() == Some(&11) && store2.get(&3).as_deref() == Some(&33));
    assert_until!(store3.get(&1).as_deref() == Some(&11) && store3.get(&2).as_deref() == Some(&22));

    // Delete key 1 on store1 and let the tombstone propagate to both other nodes.
    store1.remove(&1);
    assert_until!(store2.get(&1).is_none() && store3.get(&1).is_none());

    // Once all three converge on the tombstone state, capture the common fingerprint.
    let fp_tombstone = store1.fingerprint(..);
    assert_until!(store2.fingerprint(..) == fp_tombstone && store3.fingerprint(..) == fp_tombstone);

    task1.abort();
    task2.abort();
    task3.abort();
}

/// The same in a line topology (A↔B↔C), where propagation is relayed rather than broadcast from
/// one origin.
#[tokio::test(flavor = "multi_thread")]
async fn retained_tombstones_converge_in_3_node_cluster_line() {
    let port = 5000u16;
    let fabric = InMemoryNetwork::new();
    let net = "127.0.0.1/8".parse().unwrap();
    let addr1 = "127.0.0.113".parse().unwrap();
    let addr2 = "127.0.0.114".parse().unwrap();
    let addr3 = "127.0.0.115".parse().unwrap();

    let mk = |addr| {
        Config::default()
            .with_port(port)
            .with_listen_addr(addr)
            .with_net(net)
            .unwrap()
            .with_reconcile_interval(Duration::from_millis(100))
            .with_insecure_no_key()
    };
    // Line: A seeds B; B seeds A and C; C seeds B.
    let store1 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr1),
        Arc::new(fabric.bind(SocketAddr::new(addr1, port))),
    )
    .expect("valid test config")
    .with_seed(addr2);
    let store2 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr2),
        Arc::new(fabric.bind(SocketAddr::new(addr2, port))),
    )
    .expect("valid test config")
    .with_seed(addr1)
    .with_seed(addr3);
    let store3 = ReplicatedMap::<i32, i32>::new_with_transport(
        mk(addr3),
        Arc::new(fabric.bind(SocketAddr::new(addr3, port))),
    )
    .expect("valid test config")
    .with_seed(addr2);

    let task1 = tokio::spawn(store1.clone().run(CancellationToken::new()));
    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));
    let task3 = tokio::spawn(store3.clone().run(CancellationToken::new()));

    store1.insert(1, 11);
    store3.insert(3, 33);
    assert_until!(store1.get(&3).as_deref() == Some(&33));
    assert_until!(store3.get(&1).as_deref() == Some(&11));
    assert_until!(store2.get(&1).as_deref() == Some(&11) && store2.get(&3).as_deref() == Some(&33));

    // Delete key 1 on store1; the tombstone must reach C via B.
    store1.remove(&1);
    assert_until!(store2.get(&1).is_none() && store3.get(&1).is_none());

    let fp_tombstone = store1.fingerprint(..);
    assert_until!(store2.fingerprint(..) == fp_tombstone && store3.fingerprint(..) == fp_tombstone);

    task1.abort();
    task2.abort();
    task3.abort();
}
