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

use reconcile::{
    replicated_map::Config, ClusterKey, InMemoryNetwork, ReadReplicaMap, ReplicatedMap,
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const PORT: u16 = 7530;

fn key(byte: u8) -> ClusterKey {
    ClusterKey::new([byte; 32])
}

fn config(ip: IpAddr, primary: ClusterKey, also_accept: ClusterKey) -> Config {
    Config::new(PORT)
        .with_listen_addr(ip)
        .with_reconcile_interval(Duration::from_millis(10))
        .with_cluster_key_rotation(primary, also_accept)
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !condition() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replicas did not converge before the deadline");
}

#[tokio::test]
async fn mixed_primary_replicated_maps_exchange_writes_during_rotation() {
    let network = InMemoryNetwork::new();
    let a_ip: IpAddr = "127.0.0.2".parse().unwrap();
    let b_ip: IpAddr = "127.0.0.3".parse().unwrap();
    let old = key(0x11);
    let new = key(0x22);

    let a = ReplicatedMap::<u32, u32>::new_with_transport(
        config(a_ip, old.clone(), new.clone()),
        Arc::new(network.bind(SocketAddr::new(a_ip, PORT))),
    )
    .with_seed(b_ip);
    let b = ReplicatedMap::<u32, u32>::new_with_transport(
        config(b_ip, new, old),
        Arc::new(network.bind(SocketAddr::new(b_ip, PORT))),
    )
    .with_seed(a_ip);

    let a_shutdown = CancellationToken::new();
    let b_shutdown = CancellationToken::new();
    let a_task = tokio::spawn(a.clone().run(a_shutdown.clone()));
    let b_task = tokio::spawn(b.clone().run(b_shutdown.clone()));

    a.insert(1, 10);
    wait_until(|| b.get_cloned(&1) == Some(10)).await;

    b.insert(2, 20);
    wait_until(|| a.get_cloned(&2) == Some(20)).await;

    a_shutdown.cancel();
    b_shutdown.cancel();
    a_task.await.unwrap();
    b_task.await.unwrap();
}

#[tokio::test]
async fn read_replica_accepts_the_other_primary_during_rotation() {
    let network = InMemoryNetwork::new();
    let dated_ip: IpAddr = "127.0.0.4".parse().unwrap();
    let read_ip: IpAddr = "127.0.0.5".parse().unwrap();
    let old = key(0x33);
    let new = key(0x44);

    let dated = ReplicatedMap::<u32, u32>::new_with_transport(
        config(dated_ip, old.clone(), new.clone()),
        Arc::new(network.bind(SocketAddr::new(dated_ip, PORT))),
    )
    .with_seed(read_ip);
    let read = ReadReplicaMap::<u32, u32>::new_with_transport(
        config(read_ip, new, old),
        Arc::new(network.bind(SocketAddr::new(read_ip, PORT))),
    )
    .with_seed(dated_ip);

    let shutdown = CancellationToken::new();
    let dated_task = tokio::spawn(dated.clone().run(shutdown.clone()));
    let read_task = tokio::spawn(read.clone().run());

    dated.insert(7, 70);
    wait_until(|| read.get_cloned(&7) == Some(70)).await;

    shutdown.cancel();
    dated_task.await.unwrap();
    read_task.abort();
}

#[tokio::test]
async fn retired_key_is_rejected_after_the_rotation_window_closes() {
    let network = InMemoryNetwork::new();
    let old_ip: IpAddr = "127.0.0.6".parse().unwrap();
    let new_ip: IpAddr = "127.0.0.7".parse().unwrap();
    let old = key(0x55);
    let new = key(0x66);

    let old_sender = ReplicatedMap::<u32, u32>::new_with_transport(
        Config::new(PORT)
            .with_listen_addr(old_ip)
            .with_reconcile_interval(Duration::from_millis(10))
            .with_cluster_key(old),
        Arc::new(network.bind(SocketAddr::new(old_ip, PORT))),
    )
    .with_seed(new_ip);
    let settled_receiver = ReplicatedMap::<u32, u32>::new_with_transport(
        Config::new(PORT)
            .with_listen_addr(new_ip)
            .with_reconcile_interval(Duration::from_millis(10))
            .with_cluster_key(new),
        Arc::new(network.bind(SocketAddr::new(new_ip, PORT))),
    )
    .with_seed(old_ip);

    let old_shutdown = CancellationToken::new();
    let new_shutdown = CancellationToken::new();
    let old_task = tokio::spawn(old_sender.clone().run(old_shutdown.clone()));
    let new_task = tokio::spawn(settled_receiver.clone().run(new_shutdown.clone()));

    old_sender.insert(9, 90);
    sleep(Duration::from_millis(250)).await;
    assert_eq!(settled_receiver.get_cloned(&9), None);

    old_shutdown.cancel();
    new_shutdown.cancel();
    old_task.await.unwrap();
    new_task.await.unwrap();
}
