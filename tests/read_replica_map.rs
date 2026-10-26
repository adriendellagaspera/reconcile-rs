// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Interop between a dated [`ReplicatedMap`] and a dateless [`ReadReplicaMap`].
//!
//! These cover the two properties the read-replica design must guarantee:
//! 1. **Convergence**: the read replica receives the dated store's current values (timestamps
//!    dropped), reflects deletions as tombstones, and its value-only fingerprint matches the dated
//!    store's value-only projection.
//! 2. **Causal-stability safety**: a read replica is never counted as a causal-stability member, so
//!    it cannot block the dated store's tombstone garbage collection.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use reconcile::{replicated_map::Config, InMemoryNetwork, ReadReplicaMap, ReplicatedMap};

async fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}

macro_rules! assert_until {
    ( $x:expr ) => {
        assert!(wait_until(|| $x).await, stringify!($x))
    };
}

/// A dated store and a dateless read replica converge: the read replica ends up with every live
/// value (no timestamps), reflects a deletion as a tombstone, and its value-only fingerprint equals
/// the dated store's value-only projection fingerprint.
#[tokio::test(flavor = "multi_thread")]
async fn read_replica_converges_with_dated_store() {
    let port = 5000u16;
    let network = InMemoryNetwork::new();
    let net = "127.0.0.1/8".parse().unwrap();
    let dated_addr = "127.0.0.90".parse().unwrap();
    let read_replica_addr = "127.0.0.91".parse().unwrap();

    let dated = ReplicatedMap::<String, String>::new_with_transport(
        Config::default()
            .with_port(port)
            .with_listen_addr(dated_addr)
            .with_net(net)
            .unwrap()
            .with_insecure_no_key(),
        Arc::new(network.bind(SocketAddr::new(dated_addr, port))),
    )
    .expect("valid test config");
    // Seed the read replica with the dated store's address: the read replica *drives*
    // reconciliation over the value-only channel, so it just needs to know where to send.
    let read_replica = ReadReplicaMap::<String, String>::new_with_transport(
        Config::default()
            .with_port(port)
            .with_listen_addr(read_replica_addr)
            .with_net(net)
            .unwrap()
            .with_insecure_no_key(),
        Arc::new(network.bind(SocketAddr::new(read_replica_addr, port))),
    )
    .expect("valid test config")
    .with_seed(dated_addr);

    // Populate the dated store with live values and one key we will later delete.
    for i in 0..50 {
        dated.insert(format!("k{i:02}"), format!("v{i:02}"));
    }
    dated.insert("doomed".to_string(), "to be deleted".to_string());

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let read_replica_task = tokio::spawn(read_replica.clone().run());

    // The read replica receives the latest values, with timestamps dropped.
    assert_until!(read_replica.get(&"k00".to_string()).as_deref() == Some(&"v00".to_string()));
    assert_until!(read_replica.get(&"k49".to_string()).as_deref() == Some(&"v49".to_string()));
    assert_until!(
        read_replica.get(&"doomed".to_string()).as_deref() == Some(&"to be deleted".to_string())
    );

    // The value-only fingerprints converge: the read replica's tree fingerprints identically to
    // the dated store's value-only projection, even though it never stored a single timestamp.
    assert_until!(read_replica.value_fingerprint(..) == dated.value_fingerprint(..));
    assert_eq!(read_replica.len(), 51);

    // A fresh write on the dated store propagates to the read replica.
    dated.insert("late".to_string(), "arrival".to_string());
    assert_until!(read_replica.get(&"late".to_string()).as_deref() == Some(&"arrival".to_string()));

    // A deletion on the dated store is reflected as a tombstone: the value disappears from `get`.
    dated.remove(&"doomed".to_string());
    assert_until!(read_replica.get(&"doomed".to_string()).is_none());

    // ...and the value-only fingerprints reconverge after the deletion.
    assert_until!(read_replica.value_fingerprint(..) == dated.value_fingerprint(..));

    dated_task.abort();
    read_replica_task.abort();
}

/// A read replica must never join the dated store's causal-stability membership set, otherwise it
/// would create a receipt obligation it can never satisfy. Retention expiry still does not
/// authorize physical collection when that membership set is empty.
#[tokio::test(flavor = "multi_thread")]
async fn read_replica_is_not_a_receipt_member_and_retained_deletions_propagate() {
    let port = 5000u16;
    let network = InMemoryNetwork::new();
    let net = "127.0.0.1/8".parse().unwrap();
    let dated_addr = "127.0.0.92".parse().unwrap();
    let read_replica_addr = "127.0.0.93".parse().unwrap();

    // Aggressive tombstone expiry so GC would fire quickly *if* it is not gated.
    let dated = ReplicatedMap::<i32, i32>::new_with_transport(
        Config::default()
            .with_port(port)
            .with_listen_addr(dated_addr)
            .with_net(net)
            .unwrap()
            .with_insecure_no_key(),
        Arc::new(network.bind(SocketAddr::new(dated_addr, port))),
    )
    .expect("valid test config")
    .with_tombstone_timeout(Duration::from_millis(50));
    let read_replica = ReadReplicaMap::<i32, i32>::new_with_transport(
        Config::default()
            .with_port(port)
            .with_listen_addr(read_replica_addr)
            .with_net(net)
            .unwrap()
            .with_insecure_no_key(),
        Arc::new(network.bind(SocketAddr::new(read_replica_addr, port))),
    )
    .expect("valid test config")
    .with_seed(dated_addr);

    dated.insert(1, 11);
    dated.insert(2, 22);

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let read_replica_task = tokio::spawn(read_replica.clone().run());

    // The read replica syncs (and thereby contacts the dated store, the moment it could wrongly
    // become a member).
    assert_until!(read_replica.get(&1).as_deref() == Some(&11));
    assert_until!(read_replica.get(&2).as_deref() == Some(&22));

    assert!(!dated.members().contains(&read_replica_addr));
    dated.remove(&1);
    let with_tombstone = dated.fingerprint(..);
    assert_until!(read_replica.get(&1).is_none());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(dated.fingerprint(..), with_tombstone);
    assert!(dated.snapshot().get(&1).unwrap().is_tombstone());
    assert_eq!(dated.get(&2).as_deref(), Some(&22));
    assert!(!dated.members().contains(&read_replica_addr));

    dated_task.abort();
    read_replica_task.abort();
}

/// The same convergence contract as `read_replica_converges_with_dated_store`, over an
/// [`InMemoryNetwork`](reconcile::InMemoryNetwork) with no sockets — the full value-only
/// round-trip, not merely construction.
#[tokio::test(flavor = "multi_thread")]
async fn read_replica_converges_with_dated_store_over_in_memory_transport() {
    let network = InMemoryNetwork::new();
    let port = 5300u16;
    let net = "127.0.0.1/8".parse().unwrap();
    let dated_ip: IpAddr = "127.0.0.94".parse().unwrap();
    let read_replica_ip: IpAddr = "127.0.0.95".parse().unwrap();

    let config = |ip: IpAddr| {
        Config::default()
            .with_port(port)
            .with_listen_addr(ip)
            .with_net(net)
            .unwrap()
            .with_insecure_no_key()
    };
    let dated = ReplicatedMap::<String, String>::new_with_transport(
        config(dated_ip),
        Arc::new(network.bind(SocketAddr::new(dated_ip, port))),
    )
    .unwrap();
    // As with real sockets, only the read replica needs a seed: it drives the value-only channel
    // and the dated store answers reactively to the sender address.
    let read_replica = ReadReplicaMap::<String, String>::new_with_transport(
        config(read_replica_ip),
        Arc::new(network.bind(SocketAddr::new(read_replica_ip, port))),
    )
    .unwrap()
    .with_seed(dated_ip);

    for i in 0..50 {
        dated.insert(format!("k{i:02}"), format!("v{i:02}"));
    }
    dated.insert("doomed".to_string(), "to be deleted".to_string());

    let dated_task = tokio::spawn(dated.clone().run(CancellationToken::new()));
    let read_replica_task = tokio::spawn(read_replica.clone().run());

    // Values arrive, timestamps dropped.
    assert_until!(read_replica.get(&"k00".to_string()).as_deref() == Some(&"v00".to_string()));
    assert_until!(read_replica.get(&"k49".to_string()).as_deref() == Some(&"v49".to_string()));
    assert_until!(
        read_replica.get(&"doomed".to_string()).as_deref() == Some(&"to be deleted".to_string())
    );
    // The dateless read replica hashes identically to the dated store's value-only projection.
    assert_until!(read_replica.value_fingerprint(..) == dated.value_fingerprint(..));
    assert_eq!(read_replica.len(), 51);

    // A write made after convergence still propagates over the injected transport.
    dated.insert("late".to_string(), "arrival".to_string());
    assert_until!(read_replica.get(&"late".to_string()).as_deref() == Some(&"arrival".to_string()));

    // A deletion is reflected as a tombstone: `get` stops returning the value, the key stays as an
    // entry, and the value-only fingerprints reconverge.
    dated.remove(&"doomed".to_string());
    assert_until!(read_replica.get(&"doomed".to_string()).is_none());
    assert!(!read_replica.contains_key(&"doomed".to_string()));
    assert_until!(read_replica.value_fingerprint(..) == dated.value_fingerprint(..));

    dated_task.abort();
    read_replica_task.abort();
}
