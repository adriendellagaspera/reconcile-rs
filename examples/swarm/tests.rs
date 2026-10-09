use super::super::world::Point;
use super::*;

async fn converged(cluster: &Cluster) {
    tokio::time::timeout(Duration::from_secs(90), async {
        while cluster.divergent_keys() != 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        let mut state = cluster.state();
        for key in ["nodes", "links", "truth_map", "positions"] {
            state.as_object_mut().unwrap().remove(key);
        }
        let roots: Vec<_> = cluster.nodes.iter().map(ReplicatedMap::snapshot).collect();
        let keys: BTreeSet<_> = roots
            .iter()
            .flat_map(|s| s.iter().map(|(k, _)| k.clone()))
            .collect();
        for key in keys
            .iter()
            .filter(|k| roots[1..].iter().any(|s| s.get(*k) != roots[0].get(*k)))
            .take(3)
        {
            eprintln!(
                "Divergent {key}: {:?}",
                roots.iter().map(|s| s.get(key)).collect::<Vec<_>>()
            );
        }
        panic!("real anti-entropy did not converge after healing: {state}");
    });
    for node in &cluster.nodes {
        assert_eq!(cluster.nodes[0].to_vec(), node.to_vec());
        assert_eq!(cluster.nodes[0].fingerprint(..), node.fingerprint(..));
    }
}

#[tokio::test]
async fn local_observations_stay_isolated_then_repair_over_packet_loss() {
    let mut cluster = Cluster::new(35.0, 2).unwrap();
    cluster.partition(true);
    cluster.world.reveal_contact();
    cluster.observe();
    let before = cluster.nodes[1].to_vec();
    assert!(cluster.nodes[0].contains_key(&"contact/01/00".into()));
    assert!(!cluster.nodes[1].contains_key(&"contact/01/00".into()));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(before, cluster.nodes[1].to_vec());
    assert!(cluster.state()["partition_dropped"].as_u64().unwrap() > 0);
    cluster.heal();
    converged(&cluster).await;
    assert!(cluster.state()["loss_dropped"].as_u64().unwrap() > 0);
    assert!(cluster.state()["delivered_bytes"].as_u64().unwrap() > 0);

    cluster.partition(true);
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.world.ticks = 80;
    cluster.world.move_vehicles();
    cluster.observe();
    let a = cluster.nodes[0].to_vec();
    let b = cluster.nodes[1].to_vec();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(a, cluster.nodes[0].to_vec());
    assert_eq!(b, cluster.nodes[1].to_vec());
    assert_ne!(a, b);
    cluster.heal();
    converged(&cluster).await;
}

#[tokio::test]
async fn concurrent_contact_updates_use_the_real_lww_order() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.partition(true);
    let key = "contact/01".to_string();
    for id in 0..2 {
        cluster.nodes[id].insert(
            key.clone(),
            Observation::Contact {
                position: Point {
                    x: 10.0 + id as f64,
                    y: 7.0,
                },
                seen: 2,
                source: id,
            },
        );
    }
    let snapshots: Vec<_> = cluster.nodes.iter().map(|n| n.snapshot()).collect();
    let winner = snapshots
        .iter()
        .filter_map(|s| s.get(&key))
        .max_by_key(|e| e.stamp)
        .unwrap()
        .value()
        .unwrap()
        .clone();
    cluster.heal();
    converged(&cluster).await;
    for node in &cluster.nodes {
        assert_eq!(node.get_cloned(&key), Some(winner.clone()));
    }
}

#[tokio::test]
async fn two_nodes_converge_with_fifty_percent_loss() {
    let mut cluster = Cluster::new(50.0, 2).unwrap();
    cluster.heal();
    converged(&cluster).await;
    assert!(cluster.state()["loss_dropped"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn reset_recreates_independent_replicas_and_empty_traffic_counters() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.heal();
    converged(&cluster).await;
    let initial = Cluster::new(0.0, 2).unwrap();
    cluster = Cluster::new(0.0, 2).unwrap();
    for id in 0..2 {
        assert_eq!(cluster.nodes[id].to_vec(), initial.nodes[id].to_vec());
    }
    assert_eq!(cluster.state()["delivered_bytes"], 0);
    assert_eq!(cluster.center(), 2);
    assert_eq!(cluster.nodes.len(), 3);
    assert!(cluster.divergent_keys() > 0);
}

#[tokio::test]
async fn scripted_swarm_keeps_new_contact_in_its_group_then_converges() {
    super::super::telemetry::install().unwrap();
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let mut cluster = Cluster::new(35.0, 12).unwrap();
    cluster.start_demo();
    for _ in 0..120 {
        cluster.advance();
        tokio::task::yield_now().await;
    }
    assert_eq!(cluster.state()["partitioned"], true);
    assert!(cluster.nodes[0].contains_key(&"contact/01/00".into()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    for node in &cluster.nodes[6..] {
        assert!(!node.contains_key(&"contact/01/00".into()));
    }
    assert!(cluster.divergent_keys() > 0);
    let frozen_tick = cluster.world.ticks;
    cluster.world.playing = false;
    cluster.advance();
    assert_eq!(cluster.world.ticks, frozen_tick);
    cluster.world.playing = true;
    for _ in 120..240 {
        cluster.advance();
        tokio::task::yield_now().await;
    }
    assert_eq!(cluster.state()["partitioned"], false);
    let cut = cluster
        .nodes
        .iter()
        .map(ReplicatedMap::to_vec)
        .collect::<Vec<_>>();
    // Further scenario ticks must not produce new local observations during final repair.
    for _ in 240..300 {
        cluster.advance();
    }
    assert_eq!(
        cut,
        cluster
            .nodes
            .iter()
            .map(ReplicatedMap::to_vec)
            .collect::<Vec<_>>()
    );
    converged(&cluster).await;
    cluster.advance();
    assert!(!cluster.world.playing);
    assert!(cluster.state()["phase"]
        .as_str()
        .unwrap()
        .starts_with("5 /"));
}

#[test]
fn scenario_geometry_is_repeatable_and_patrols_stay_in_water() {
    let mut a = World::new(12);
    let mut b = World::new(12);
    for ticks in 0..240 {
        a.ticks = ticks;
        b.ticks = ticks;
        a.move_vehicles();
        b.move_vehicles();
        assert_eq!(a.positions, b.positions);
        for point in &a.positions {
            assert!(point.x >= 0.0 && point.x < WIDTH as f64);
            assert!(point.y >= 0.0 && point.y < HEIGHT as f64);
            assert!(!terrain(point.x as usize, point.y as usize));
        }
    }
}

#[tokio::test]
async fn isolated_drone_keeps_working_and_exchanges_both_ways_on_return() {
    let mut cluster = Cluster::new(0.0, 3).unwrap();
    cluster.heal();
    converged(&cluster).await;
    cluster.set_peer(0, PeerState::Offline).unwrap();
    let center_before = cluster.nodes[cluster.center()].to_vec();
    cluster.world.ticks = 100;
    cluster.world.reveal_contact();
    cluster.observe();
    let isolated_contact = "contact/01/00".to_string();
    assert!(cluster.nodes[0].contains_key(&isolated_contact));
    let remote_key = "independent-discovery".to_string();
    cluster.nodes[1].insert(
        remote_key.clone(),
        Observation::Sector {
            id: 999,
            scanned: 100,
        },
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!cluster.nodes[0].contains_key(&remote_key));
    assert!(!cluster.nodes[cluster.center()].contains_key(&isolated_contact));
    assert_ne!(center_before, cluster.nodes[cluster.center()].to_vec());
    cluster.set_peer(0, PeerState::Active).unwrap();
    converged(&cluster).await;
    assert!(cluster.nodes[0].contains_key(&remote_key));
    for node in &cluster.nodes {
        let Some(Observation::Contact { seen, .. }) = node.get_cloned(&isolated_contact) else {
            panic!("missing report")
        };
        assert_eq!(seen, 100, "reception must not refresh observation time");
    }
}

#[tokio::test]
async fn halted_peer_freezes_motion_sensors_and_protocol_then_resumes_retained_state() {
    let mut cluster = Cluster::new(0.0, 4).unwrap();
    cluster.heal();
    converged(&cluster).await;
    cluster.set_peer(0, PeerState::Stopped).unwrap();
    let position = cluster.world.positions[0];
    let before = cluster.nodes[0].to_vec();
    let rounds = cluster.nodes[0].sync_state().rounds;
    cluster.world.playing = true;
    for _ in 0..20 {
        cluster.advance();
    }
    cluster.nodes[1].insert(
        "while-stopped".into(),
        Observation::Sector {
            id: 99,
            scanned: 20,
        },
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(cluster.world.positions[0], position);
    assert_eq!(cluster.nodes[0].to_vec(), before);
    assert_eq!(cluster.nodes[0].sync_state().rounds, rounds);
    cluster.set_peer(0, PeerState::Active).unwrap();
    converged(&cluster).await;
    assert!(cluster.nodes[0].contains_key(&"while-stopped".into()));
    cluster.advance();
    assert_ne!(cluster.world.positions[0], position);
}

#[tokio::test]
async fn fleet_repairs_without_g1_g7_or_command_center() {
    let mut cluster = Cluster::new(0.0, 12).unwrap();
    cluster.heal();
    for id in [0, 6, cluster.center()] {
        cluster.set_peer(id, PeerState::Stopped).unwrap();
    }
    cluster.nodes[2].insert(
        "no-coordinator".into(),
        Observation::Sector {
            id: 101,
            scanned: 0,
        },
    );
    let active: Vec<_> = (0..12).filter(|id| ![0, 6].contains(id)).collect();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if active
                .iter()
                .all(|&id| cluster.nodes[id].to_vec() == cluster.nodes[2].to_vec())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("remaining fleet must converge without special relays");
    assert!(!cluster.nodes[cluster.center()].contains_key(&"no-coordinator".into()));
    cluster.heal();
    converged(&cluster).await;
}

#[tokio::test]
async fn source_reports_coexist_and_center_has_no_omniscient_shortcut() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster
        .set_peer(cluster.center(), PeerState::Offline)
        .unwrap();
    for id in 0..2 {
        cluster.nodes[id].insert(
            format!("contact/01/{id:02}"),
            Observation::Contact {
                position: Point {
                    x: 10.0 + id as f64,
                    y: 7.0,
                },
                seen: 8 + id as u64,
                source: id,
            },
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let center = cluster.state()["nodes"][cluster.center()].clone();
    assert_eq!(center, serde_json::json!([]));
    cluster.heal();
    converged(&cluster).await;
    for node in &cluster.nodes {
        assert!(node.contains_key(&"contact/01/00".into()));
        assert!(node.contains_key(&"contact/01/01".into()));
    }
}

#[test]
fn topology_is_pairwise_distance_based_and_cuts_are_symmetric() {
    let positions = vec![
        Point { x: 0.0, y: 0.0 },
        Point { x: 5.0, y: 0.0 },
        Point { x: 10.0, y: 0.0 },
    ];
    let network = Network::new(positions);
    network.topology.write().range = 6.0;
    assert!(network.allowed(0, 1));
    assert!(network.allowed(1, 2));
    assert!(!network.allowed(0, 2));
    assert_eq!(network.components().len(), 1);
    network.topology.write().positions[1].x = 30.0;
    assert_eq!(network.components().len(), 3);
    network.topology.write().range = 40.0;
    let mut topology = network.topology.write();
    topology.blocked[1] = true;
    topology.blocked[3] = true;
    drop(topology);
    assert!(!network.allowed(0, 1));
    assert!(!network.allowed(1, 0));
    assert!(network.allowed(0, 2));
    assert!(network.allowed(1, 2));
    assert_eq!(network.components().len(), 1);
}
