use super::super::world::{Point, HEIGHT, WIDTH};
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
        for key in ["nodes", "links", "reference_map", "positions"] {
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
    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
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

    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
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
    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
    let key = "contact/01".to_string();
    for id in 0..2 {
        cluster.nodes[id].insert(
            key.clone(),
            Observation::Contact {
                id: 1,
                kind: super::super::world::ContactKind::Hostile,
                position: Point {
                    x: 10.0 + id as f64,
                    y: 7.0,
                },
                bearing: None,
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
async fn scripted_outages_keep_isolated_contact_local_then_converge() {
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
    for node in &cluster.nodes[1..] {
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
    let frozen_contacts = serde_json::to_value(cluster.world.contacts()).unwrap();
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
    assert_eq!(
        frozen_contacts,
        serde_json::to_value(cluster.world.contacts()).unwrap()
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
            assert!(!super::super::world::terrain_at(*point));
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
        let Some(Observation::Contact {
            id: 1,
            kind: super::super::world::ContactKind::Mechanical,
            seen,
            ..
        }) = node.get_cloned(&isolated_contact)
        else {
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
                id: 1,
                kind: super::super::world::ContactKind::Hostile,
                position: Point {
                    x: 10.0 + id as f64,
                    y: 7.0,
                },
                bearing: None,
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

#[tokio::test]
async fn orders_wait_for_replication_and_acknowledgements_wait_for_return_path() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.heal();
    converged(&cluster).await;
    cluster.set_peer(0, PeerState::Offline).unwrap();
    cluster.issue_order(0, OrderAction::Hold).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    cluster.advance();
    assert!(!cluster.world.held[0]);
    assert!(!cluster.nodes[0].contains_key(&"order/00".into()));
    cluster.set_peer(0, PeerState::Active).unwrap();
    converged(&cluster).await;
    cluster.set_peer(0, PeerState::Offline).unwrap();
    cluster.advance();
    assert!(cluster.world.held[0]);
    assert!(cluster.nodes[0].contains_key(&"order-ack/00/0000000000000001".into()));
    assert!(!cluster.nodes[cluster.center()].contains_key(&"order-ack/00/0000000000000001".into()));
    let ack = cluster.nodes[0].get_cloned(&"order-ack/00/0000000000000001".into());
    cluster.world.ticks += 1;
    cluster.advance();
    assert_eq!(
        ack,
        cluster.nodes[0].get_cloned(&"order-ack/00/0000000000000001".into())
    );
    cluster.heal();
    converged(&cluster).await;
    cluster.issue_order(0, OrderAction::Patrol).unwrap();
    converged(&cluster).await;
    cluster.advance();
    assert!(!cluster.world.held[0]);
    let position = cluster.world.positions[0];
    cluster.world.playing = true;
    cluster.advance();
    assert_ne!(position, cluster.world.positions[0]);
}

#[tokio::test]
async fn halted_drone_rejects_expired_orders_and_halted_center_cannot_issue() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.heal();
    cluster.set_peer(0, PeerState::Stopped).unwrap();
    cluster.issue_order(0, OrderAction::Hold).unwrap();
    cluster.world.ticks = 241;
    cluster.advance();
    assert!(!cluster.world.held[0]);
    cluster.set_peer(0, PeerState::Active).unwrap();
    converged(&cluster).await;
    cluster.advance();
    assert!(!cluster.world.held[0]);
    assert!(matches!(
        cluster.nodes[0].get_cloned(&"order-ack/00/0000000000000001".into()),
        Some(Observation::Acknowledgement { applied: false, .. })
    ));
    cluster
        .set_peer(cluster.center(), PeerState::Stopped)
        .unwrap();
    assert!(cluster.issue_order(0, OrderAction::Scan).is_err());
    assert!(cluster
        .issue_order(cluster.center(), OrderAction::Scan)
        .is_err());
}

#[tokio::test]
async fn relayed_knowledge_does_not_mark_its_source_as_a_direct_neighbor() {
    let mut cluster = Cluster::new(0.0, 3).unwrap();
    cluster.heal();
    cluster
        .set_peer(cluster.center(), PeerState::Stopped)
        .unwrap();
    cluster.toggle_link(0, 2).unwrap();
    cluster.nodes[2].insert("relayed".into(), Observation::Sector { id: 99, scanned: 0 });
    tokio::time::timeout(Duration::from_secs(30), async {
        while !cluster.nodes[0].contains_key(&"relayed".into()) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let direct = cluster.network.direct_peers(0);
    assert!(direct.iter().any(|p| p.peer == 1));
    assert!(!direct.iter().any(|p| p.peer == 2));
}

#[test]
fn typed_contacts_and_fine_coast_are_repeatable_and_stay_in_water() {
    let mut world = World::new(12);
    let other = World::new(12);
    assert_eq!(
        serde_json::to_value(world.contacts()).unwrap(),
        serde_json::to_value(other.contacts()).unwrap()
    );
    assert_eq!(world.contacts().len(), 5);
    for ticks in 0..1000 {
        world.ticks = ticks;
        for contact in world.contacts() {
            assert!(!super::super::world::terrain_at(contact.position));
        }
    }
    assert!((0..HEIGHT).any(|y| (0..WIDTH).any(|x| {
        let detail = super::super::world::terrain_detail(x, y);
        detail.iter().any(|row| *row != 0) && detail.iter().any(|row| *row != 255)
    })));
}

#[tokio::test]
async fn capped_network_delivers_orders_through_real_reconciliation() {
    let mut cluster = Cluster::with_limits(0.0, 2, 1200, 10).unwrap();
    cluster.heal();
    cluster.issue_order(0, OrderAction::Hold).unwrap();
    assert!(!cluster.world.held[0]);
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            cluster.advance();
            if matches!(
                cluster.nodes[cluster.center()].get_cloned(&"order-ack/00/0000000000000001".into()),
                Some(Observation::Acknowledgement { applied: true, .. })
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a quiescent capped fleet must eventually acknowledge a command");
    assert!(cluster.world.held[0]);
    let state = cluster.state();
    assert_eq!(state["bandwidth_kbps"], 10);
    assert!(state["bandwidth"]
        .as_array()
        .unwrap()
        .iter()
        .all(|stats| stats["queued_bytes"].as_u64().unwrap()
            <= super::super::bandwidth::QUEUE_BYTES as u64));
    assert!(state["bandwidth"][0]["tx_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn immutable_order_history_and_acknowledgements_survive_new_desired_orders() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.heal();
    converged(&cluster).await;
    cluster.issue_order(0, OrderAction::Hold).unwrap();
    converged(&cluster).await;
    cluster.advance();
    let ack_one = "order-ack/00/0000000000000001".to_string();
    assert!(cluster.nodes[0].contains_key(&ack_one));
    cluster.heal();
    converged(&cluster).await;

    cluster.issue_order(0, OrderAction::Patrol).unwrap();
    converged(&cluster).await;
    cluster.advance();
    let ack_two = "order-ack/00/0000000000000002".to_string();
    assert!(cluster.nodes[0].contains_key(&ack_one));
    assert!(cluster.nodes[0].contains_key(&ack_two));
    assert!(
        cluster.nodes[cluster.center()].contains_key(&"order-issued/00/0000000000000001".into())
    );
    assert!(
        cluster.nodes[cluster.center()].contains_key(&"order-issued/00/0000000000000002".into())
    );
    assert!(matches!(
        cluster.nodes[0].get_cloned(&"order/00".into()),
        Some(Observation::Order { sequence: 2, .. })
    ));
    converged(&cluster).await;
}

#[tokio::test]
async fn first_contact_discovery_is_immutable_while_latest_report_changes() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    cluster.world.ticks = 20;
    cluster.world.reveal_contact();
    cluster.observe();
    let first_key = "contact-first/01/00".to_string();
    let latest_key = "contact/01/00".to_string();
    let first = cluster.nodes[0]
        .get_cloned(&first_key)
        .expect("first discovery recorded");
    assert!(matches!(first, Observation::Contact { seen: 20, .. }));
    cluster.world.ticks = 40;
    cluster.observe();
    assert_eq!(cluster.nodes[0].get_cloned(&first_key), Some(first));
    assert!(matches!(
        cluster.nodes[0].get_cloned(&latest_key),
        Some(Observation::Contact { seen: 40, .. })
    ));
}

#[test]
fn command_station_is_on_land() {
    assert!(super::super::world::terrain_at(COMMAND_POSITION));
}

#[tokio::test]
async fn reference_chart_is_shared_without_replica_entries_or_sector_coverage() {
    let mut cluster = Cluster::new(0.0, 3).unwrap();
    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
    let chart = cluster.state()["reference_map"].clone();
    assert_eq!(chart["detail"].as_array().unwrap().len(), 640);
    assert_eq!(chart["km_per_unit"], 1.0);
    assert!(cluster.nodes[cluster.center()].is_empty());
    for node in &cluster.nodes {
        assert!(node
            .to_vec()
            .iter()
            .all(|(key, _)| !key.starts_with("map/")));
    }
    cluster.world.ticks = 20;
    cluster.observe();
    for node in &cluster.nodes {
        assert!(node
            .to_vec()
            .iter()
            .all(|(key, _)| !key.starts_with("sector/") && !key.starts_with("map/")));
    }
    assert!(matches!(
        cluster.nodes[0].get_cloned(&"vehicle/00".into()),
        Some(Observation::Vehicle { seen: 20, .. })
    ));
    assert_eq!(chart, cluster.state()["reference_map"]);
    assert!(cluster.nodes[cluster.center()].is_empty());
}

#[tokio::test]
async fn coastal_access_requires_intermediate_replicas_for_offshore_reports_and_commands() {
    let mut cluster = Cluster::with_limits(0.0, 3, 1200, 10).unwrap();
    cluster.world.positions = vec![
        Point { x: 6.0, y: 10.0 },
        Point { x: 13.0, y: 10.0 },
        Point { x: 20.0, y: 10.0 },
    ];
    cluster.set_range(8.0).unwrap();
    let center = cluster.center();
    assert!(cluster.network.allowed(center, 0));
    assert!(!cluster.network.allowed(center, 1));
    assert!(!cluster.network.allowed(center, 2));
    assert!(!cluster.network.allowed(0, 2));
    assert_eq!(cluster.network.components().len(), 1);
    cluster.toggle_link(0, 1).unwrap();
    let key = "contact/99/02".to_string();
    let report = Observation::Contact {
        id: 99,
        kind: super::super::world::ContactKind::Hostile,
        position: Point { x: 20.0, y: 10.0 },
        bearing: None,
        seen: 0,
        source: 2,
    };
    cluster.nodes[2].insert(key.clone(), report.clone());
    tokio::time::timeout(Duration::from_secs(60), async {
        while !cluster.nodes[1].contains_key(&key) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("intermediate drone must integrate the offshore report");
    assert!(!cluster.nodes[center].contains_key(&key));
    cluster.set_peer(2, PeerState::Offline).unwrap();
    cluster.toggle_link(0, 1).unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !cluster.nodes[center].contains_key(&key) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("retained observation must reach CC while its source is offline");
    assert_eq!(cluster.nodes[center].get_cloned(&key), Some(report));
    cluster.issue_order(2, OrderAction::Hold).unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !cluster.nodes[1].contains_key(&"order/02".into()) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("intermediate drone must integrate a command for another recipient");
    cluster.advance();
    assert!(!cluster.world.held.iter().any(|held| *held));
    cluster.set_peer(2, PeerState::Active).unwrap();
    let ack = "order-ack/02/0000000000000001".to_string();
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            cluster.advance();
            if cluster.nodes[center].contains_key(&ack) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("desired command and acknowledgement must traverse the chain");
    assert_eq!(cluster.world.held, vec![false, false, true]);
    assert!(matches!(
        cluster.nodes[center].get_cloned(&ack),
        Some(Observation::Acknowledgement {
            recipient: 2,
            applied: true,
            ..
        })
    ));
    assert!(cluster
        .network
        .direct_peers(center)
        .iter()
        .all(|peer| peer.peer == 0));
    assert!(cluster
        .network
        .direct_peers(2)
        .iter()
        .all(|peer| peer.peer == 1));
}

#[test]
fn default_fleet_has_coastal_neighbors_and_offshore_multi_hop_paths() {
    let world = World::new(12);
    let mut positions = world.positions;
    positions.push(COMMAND_POSITION);
    let network = Network::new(positions);
    network.topology.write().coastal_position = Some(COASTAL_POSITION);
    let direct = (0..12).filter(|&peer| network.allowed(12, peer)).count();
    assert!(direct > 0 && direct < 12);
    assert_eq!(network.components().len(), 1);
    assert!(super::super::world::terrain_at(COMMAND_POSITION));
    assert!(!super::super::world::terrain_at(COASTAL_POSITION));
}

#[tokio::test]
async fn listening_envelope_and_bearings_never_reveal_truth_identity_or_position() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
    cluster.world.reveal_contact();
    cluster.observe();
    let truth = cluster.world.contact().unwrap();
    let Some(Observation::Contact {
        kind,
        position,
        bearing: Some(bearing),
        ..
    }) = cluster.nodes[0].get_cloned(&"contact/01/00".into())
    else {
        panic!("missing bearing")
    };
    assert_eq!(kind, ContactKind::Mechanical);
    assert_eq!(position, bearing.anchor());
    assert_ne!(position, truth);
    assert!(bearing.origin.distance(truth) <= bearing.range_km);
    let actual = (truth.x - bearing.origin.x)
        .atan2(-(truth.y - bearing.origin.y))
        .to_degrees();
    let error = (actual - bearing.direction_deg + 180.0).rem_euclid(360.0) - 180.0;
    assert!(error.abs() <= bearing.half_angle_deg);
    assert!(!cluster.nodes[1].contains_key(&"contact/01/01".into()));
    cluster.world.positions[1] = Point {
        x: truth.x + 2.0,
        y: truth.y,
    };
    cluster.observe_peer(1, true);
    assert!(!cluster.nodes[1].contains_key(&"contact/01/01".into()));
    cluster.world.positions[1].x = truth.x + 1.0;
    cluster.observe_peer(1, true);
    assert!(cluster.nodes[1].contains_key(&"contact/01/01".into()));
}

#[tokio::test]
async fn capped_handover_retains_old_source_and_delivers_new_source_without_cc_shortcuts() {
    let mut cluster = Cluster::with_limits(0.0, 3, 1200, 10).unwrap();
    for id in 0..cluster.nodes.len() {
        cluster.set_peer(id, PeerState::Offline).unwrap();
    }
    cluster.world.positions[1] = Point { x: 7.0, y: 3.0 };
    cluster.world.ticks = 20;
    cluster.world.reveal_contact();
    cluster.observe();
    let old_key = "contact/01/00".to_string();
    let next_key = "contact/01/01".to_string();
    let old = cluster.nodes[0].get_cloned(&old_key).unwrap();
    cluster.set_peer(0, PeerState::Stopped).unwrap();
    cluster.world.ticks = 40;
    cluster.observe();
    let next = cluster.nodes[1].get_cloned(&next_key).unwrap();
    assert!(matches!(
        next,
        Observation::Contact {
            source: 1,
            seen: 40,
            bearing: Some(_),
            ..
        }
    ));
    assert_eq!(cluster.nodes[0].get_cloned(&old_key), Some(old.clone()));
    assert!(cluster.nodes[cluster.center()].is_empty());
    cluster.heal();
    converged(&cluster).await;
    for node in &cluster.nodes {
        assert_eq!(node.get_cloned(&old_key), Some(old.clone()));
        assert_eq!(node.get_cloned(&next_key), Some(next.clone()));
    }
}

#[tokio::test]
async fn mission_clock_acceleration_preserves_pause_and_validates_rates() {
    let mut cluster = Cluster::new(0.0, 2).unwrap();
    assert!(super::super::controls::apply(&mut cluster, "speed/7").is_err());
    super::super::controls::apply(&mut cluster, "speed/5").unwrap();
    cluster.world.playing = true;
    cluster.advance();
    assert_eq!(cluster.world.ticks, 5);
    cluster.world.playing = false;
    cluster.advance();
    assert_eq!(cluster.world.ticks, 5);
    super::super::controls::apply(&mut cluster, "reset").unwrap();
    assert_eq!(cluster.world.speed, 5);
}

#[test]
fn corridor_contact_moves_between_watch_stations_at_a_bounded_speed() {
    let mut world = World::new(12);
    world.reveal_contact();
    let start = world.contact().unwrap();
    assert!(start.distance(world.positions[0]) <= LISTENING_RANGE_KM);
    world.ticks = 800;
    let next = world.contact().unwrap();
    assert!(next.distance(world.positions[0]) > LISTENING_RANGE_KM);
    assert!(world.positions[1..]
        .iter()
        .any(|p| next.distance(*p) <= LISTENING_RANGE_KM));
    assert!(start.distance(next) <= world.seconds() * 0.008 + f64::EPSILON);
    assert!(!super::super::world::terrain_at(next));
}
