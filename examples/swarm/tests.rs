use super::*;

async fn converged(cluster: &Cluster) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while cluster.state()["divergent_keys"].as_u64() != Some(0) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real anti-entropy must converge after healing");
    assert_eq!(cluster.nodes[0].to_vec(), cluster.nodes[1].to_vec());
    assert_eq!(
        cluster.nodes[0].fingerprint(..),
        cluster.nodes[1].fingerprint(..)
    );
}

#[tokio::test]
async fn local_observations_stay_isolated_then_repair_over_packet_loss() {
    let mut cluster = Cluster::new(35.0).unwrap();
    let before = cluster.nodes[1].to_vec();
    assert!(cluster.nodes[1].get(&"contact/01".into()).is_none());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(before, cluster.nodes[1].to_vec());
    assert!(cluster.state()["partition_dropped"].as_u64().unwrap() > 0);
    cluster.partition(false);
    converged(&cluster).await;
    assert!(cluster.state()["loss_dropped"].as_u64().unwrap() > 0);
    assert!(cluster.state()["delivered_bytes"].as_u64().unwrap() > 0);

    cluster.partition(true);
    // Drain packets delivered before the topology change before taking the isolation baseline.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.observe();
    let a = cluster.nodes[0].to_vec();
    let b = cluster.nodes[1].to_vec();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(a, cluster.nodes[0].to_vec());
    assert_eq!(b, cluster.nodes[1].to_vec());
    assert_ne!(a, b);
    cluster.partition(false);
    converged(&cluster).await;
}

#[tokio::test]
async fn concurrent_contact_updates_use_the_real_lww_order() {
    let cluster = Cluster::new(0.0).unwrap();
    let key = "contact/01".to_string();
    cluster.nodes[1].insert(
        key.clone(),
        Observation::Contact {
            x: 25,
            y: 11,
            seen: 2,
            source: 1,
        },
    );
    let snapshots: Vec<_> = cluster.nodes.iter().map(|n| n.snapshot()).collect();
    let winner = snapshots
        .iter()
        .map(|s| s.get(&key).unwrap())
        .max_by_key(|e| e.stamp)
        .unwrap()
        .value()
        .unwrap()
        .clone();
    cluster.partition(false);
    converged(&cluster).await;
    for node in &cluster.nodes {
        assert_eq!(node.get_cloned(&key), Some(winner.clone()));
    }
}

#[tokio::test]
async fn reset_recreates_independent_replicas_and_empty_traffic_counters() {
    let mut cluster = Cluster::new(0.0).unwrap();
    cluster.partition(false);
    converged(&cluster).await;
    let initial = Cluster::new(0.0).unwrap();
    cluster = Cluster::new(0.0).unwrap();
    for id in 0..2 {
        assert_eq!(cluster.nodes[id].to_vec(), initial.nodes[id].to_vec());
    }
    assert_eq!(cluster.state()["delivered_bytes"], 0);
    assert_eq!(cluster.state()["partitioned"], true);
    assert!(cluster.state()["divergent_keys"].as_u64().unwrap() > 0);
}
