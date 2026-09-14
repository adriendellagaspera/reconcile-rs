// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reconcile::{replicated_map::Config, ClusterKey, InMemoryNetwork, ReplicatedMap};
use tokio_util::sync::CancellationToken;

fn base_config(ip: IpAddr, port: u16) -> Config {
    Config::default()
        .with_listen_addr(ip)
        .with_port(port)
        .with_reconcile_interval(Duration::from_millis(5))
}

fn config(ip: IpAddr, port: u16, key: ClusterKey) -> Config {
    base_config(ip, port).with_cluster_key(key)
}

fn rotating_config(
    ip: IpAddr,
    port: u16,
    primary: ClusterKey,
    also_accept: ClusterKey,
) -> Config {
    base_config(ip, port).with_cluster_key_rotation(primary, also_accept)
}

async fn wait_until(deadline: Instant, mut predicate: impl FnMut() -> bool) -> bool {
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    predicate()
}

#[tokio::test]
async fn mixed_primary_keys_converge_inside_the_two_key_window() {
    let network = InMemoryNetwork::new();
    let port = 5400;
    let a_ip: IpAddr = "127.0.0.21".parse().unwrap();
    let b_ip: IpAddr = "127.0.0.22".parse().unwrap();
    let old = ClusterKey::new([0x11; 32]);
    let new = ClusterKey::new([0x22; 32]);

    // Phase 2 of the documented rollout: A has switched its send key to `new`, while B still
    // sends with `old`. Both accept the other key on receive, so neither direction is cut off.
    let a = ReplicatedMap::<u32, u32>::new_with_transport(
        rotating_config(a_ip, port, new.clone(), old.clone()),
        Arc::new(network.bind(SocketAddr::new(a_ip, port))),
    );
    let b = ReplicatedMap::<u32, u32>::new_with_transport(
        rotating_config(b_ip, port, old.clone(), new.clone()),
        Arc::new(network.bind(SocketAddr::new(b_ip, port))),
    );

    // Populate before seeding so convergence must come from anti-entropy, not the eager write
    // broadcast. This also exercises the keyed-fingerprint mismatch that exists transiently while
    // the two nodes have different primaries (#114): values must still converge.
    a.insert(1, 10);
    b.insert(2, 20);
    let a = a.with_seed(b_ip);
    let b = b.with_seed(a_ip);

    let shutdown = CancellationToken::new();
    let a_task = tokio::spawn(a.clone().run(shutdown.clone()));
    let b_task = tokio::spawn(b.clone().run(shutdown.clone()));

    let converged = wait_until(Instant::now() + Duration::from_secs(2), || {
        a.get_cloned(&2) == Some(20) && b.get_cloned(&1) == Some(10)
    })
    .await;

    shutdown.cancel();
    a_task.await.unwrap();
    b_task.await.unwrap();

    assert!(
        converged,
        "mixed-primary peers did not converge inside the rotation window"
    );
}

#[tokio::test]
async fn retired_key_is_rejected_after_the_window_closes() {
    let network = InMemoryNetwork::new();
    let port = 5401;
    let old_ip: IpAddr = "127.0.0.23".parse().unwrap();
    let new_ip: IpAddr = "127.0.0.24".parse().unwrap();
    let old = ClusterKey::new([0x33; 32]);
    let new = ClusterKey::new([0x44; 32]);

    let old_node = ReplicatedMap::<u32, u32>::new_with_transport(
        config(old_ip, port, old),
        Arc::new(network.bind(SocketAddr::new(old_ip, port))),
    );
    let new_node = ReplicatedMap::<u32, u32>::new_with_transport(
        config(new_ip, port, new),
        Arc::new(network.bind(SocketAddr::new(new_ip, port))),
    );

    old_node.insert(7, 70);
    let old_node = old_node.with_seed(new_ip);
    let new_node = new_node.with_seed(old_ip);

    let shutdown = CancellationToken::new();
    let old_task = tokio::spawn(old_node.clone().run(shutdown.clone()));
    let new_task = tokio::spawn(new_node.clone().run(shutdown.clone()));

    // Several reconciliation intervals are enough to prove traffic is being attempted; every
    // old-key datagram must fail authentication once the new node has dropped its accepted key.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(new_node.get_cloned(&7), None);

    shutdown.cancel();
    old_task.await.unwrap();
    new_task.await.unwrap();
}
