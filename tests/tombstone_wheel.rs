//! Retention expiry and an empty receipt quorum do not authorize physical deletion.
//! Same-instant wheel indexing is covered by the wheel's unit tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reconcile::{replicated_map::Config, Fingerprint, InMemoryNetwork, ReplicatedMap};
use tokio_util::sync::CancellationToken;

async fn retained_after_expiry(keys: &[i32]) {
    let fabric = InMemoryNetwork::new();
    let address: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let store = ReplicatedMap::<i32, i32>::new_with_transport(
        Config::default()
            .with_listen_addr(address.ip())
            .with_port(address.port())
            .with_insecure_no_key(),
        Arc::new(fabric.bind(address)),
    )
    .unwrap()
    .with_tombstone_timeout(Duration::ZERO);
    for &key in keys {
        store.insert(key, key * 10);
    }
    store.remove_bulk(keys);
    let fingerprint = store.fingerprint(..);
    assert_ne!(fingerprint, Fingerprint::ZERO);
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(store.clone().run(shutdown.clone()));
    // Observe the public run loop past the historical sweep interval. The internal single-poll
    // test separately proves the closed gate without inferring lock safety from elapsed time.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(store.fingerprint(..), fingerprint);
    for &key in keys {
        assert!(store.get(&key).is_none());
        assert!(store.snapshot().get(&key).unwrap().is_tombstone());
    }
    shutdown.cancel();
    assert!(running.await.unwrap().final_snapshot.is_ok());
}

#[tokio::test]
async fn bulk_tombstones_remain_after_retention_without_a_history_proof() {
    retained_after_expiry(&(0..8).collect::<Vec<_>>()).await;
}

#[tokio::test]
async fn a_single_tombstone_remains_after_retention_without_a_history_proof() {
    retained_after_expiry(&[99]).await;
}
