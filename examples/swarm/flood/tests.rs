use super::*;

#[tokio::test]
async fn decisions_use_only_received_reports_and_corrections_cancel_verification() {
    let mut c = Cluster::new(0.0, 6).unwrap();
    c.start_flood();
    for id in 0..c.nodes.len() {
        c.set_peer(id, PeerState::Offline).unwrap();
    }
    let report = Observation::Poi {
        id: 1,
        kind: PoiKind::People,
        finding: Finding::Suspected,
        position: Point { x: 10.6, y: 6.0 },
        seen: 1,
        source: 0,
    };
    c.nodes[0].insert("poi/01/00".into(), report.clone());
    c.advance();
    assert_eq!(c.flood.as_ref().unwrap().aircraft[1].role, Role::Search);
    assert!(c.nodes[c.center()].is_empty());
    let before_verification = c.world.positions[1];
    c.nodes[1].insert("poi/01/00".into(), report);
    c.advance();
    assert_eq!(c.flood.as_ref().unwrap().aircraft[1].role, Role::Verify);
    assert!(
        c.world.positions[1].distance(Point { x: 10.6, y: 6.0 })
            < before_verification.distance(Point { x: 10.6, y: 6.0 })
    );
    c.nodes[1].insert(
        "poi/01/03".into(),
        Observation::Poi {
            id: 1,
            kind: PoiKind::People,
            finding: Finding::Dismissed,
            position: Point { x: 10.6, y: 6.0 },
            seen: 0,
            source: 3,
        },
    );
    c.advance();
    assert_eq!(c.flood.as_ref().unwrap().aircraft[1].role, Role::Search);
}

#[tokio::test]
async fn camera_observations_require_footprint_and_thermal_verification() {
    let mut c = Cluster::new(0.0, 6).unwrap();
    c.start_flood();
    for id in 0..c.nodes.len() {
        c.set_peer(id, PeerState::Offline).unwrap();
    }
    let p = c.flood.as_ref().unwrap().pois[0].position;
    c.world.positions[0] = Point {
        x: p.x + FOOTPRINT_KM * 2.0,
        y: p.y,
    };
    c.observe_flood(0, true);
    assert!(!c.nodes[0].contains_key(&"poi/01/00".into()));
    c.world.positions[0] = p;
    c.observe_flood(0, true);
    assert!(matches!(
        c.nodes[0].get_cloned(&"poi/01/00".into()),
        Some(Observation::Poi {
            finding: Finding::Suspected,
            ..
        })
    ));
    c.world.positions[1] = p;
    c.observe_flood(1, true);
    c.world.ticks = 20;
    c.observe_flood(1, true);
    assert!(matches!(
        c.nodes[1].get_cloned(&"poi/01/01".into()),
        Some(Observation::Poi {
            finding: Finding::Confirmed,
            seen: 20,
            ..
        })
    ));
    assert!(!c.nodes[c.center()].contains_key(&"poi/01/01".into()));
}

#[tokio::test]
async fn flight_speed_pause_and_reserve_are_bounded() {
    let mut c = Cluster::new(0.0, 6).unwrap();
    c.start_flood();
    let before = c.world.positions.clone();
    c.advance();
    for (a, b) in before.iter().zip(&c.world.positions) {
        assert!(a.distance(*b) <= CRUISE_KM_S * 0.5 + 1e-9);
    }
    c.world.playing = false;
    let p = c.world.positions.clone();
    c.advance();
    assert_eq!(p, c.world.positions);
    c.world.playing = true;
    c.world.ticks = 6000;
    c.advance();
    assert!(c
        .flood
        .as_ref()
        .unwrap()
        .aircraft
        .iter()
        .all(|a| a.role == Role::Return));
}

#[tokio::test]
async fn real_reconciliation_delivers_finding_and_enables_local_reaction() {
    let mut c = Cluster::new(0.0, 3).unwrap();
    c.start_flood();
    c.set_peer(1, PeerState::Offline).unwrap();
    c.set_peer(c.center(), PeerState::Offline).unwrap();
    let key = "poi/01/00".to_string();
    let report = Observation::Poi {
        id: 1,
        kind: PoiKind::People,
        finding: Finding::Suspected,
        position: Point { x: 10.6, y: 6.0 },
        seen: 50,
        source: 0,
    };
    c.nodes[0].insert(key.clone(), report.clone());
    c.advance();
    assert_eq!(c.flood.as_ref().unwrap().aircraft[1].role, Role::Search);
    c.heal();
    tokio::time::timeout(Duration::from_secs(30), async {
        while c.nodes[1].get_cloned(&key) != Some(report.clone())
            || c.nodes[c.center()].get_cloned(&key) != Some(report.clone())
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    c.advance();
    assert_eq!(c.flood.as_ref().unwrap().aircraft[1].role, Role::Verify);
    assert_eq!(c.nodes[c.center()].get_cloned(&key), Some(report));
}

#[tokio::test]
async fn false_positive_verification_is_local_and_cancels_its_own_detour() {
    let mut c = Cluster::new(0.0, 6).unwrap();
    c.start_flood();
    for id in 0..c.nodes.len() {
        c.set_peer(id, PeerState::Offline).unwrap();
    }
    for _ in 0..110 {
        c.advance();
    }
    assert!(matches!(
        c.nodes[3].get_cloned(&"poi/03/03".into()),
        Some(Observation::Poi {
            finding: Finding::Dismissed,
            ..
        })
    ));
    assert_eq!(c.flood.as_ref().unwrap().aircraft[3].role, Role::Search);
    assert!(matches!(
        c.nodes[3].get_cloned(&"poi-first/03/03".into()),
        Some(Observation::Poi {
            finding: Finding::Suspected,
            ..
        })
    ));
    assert!(!c.nodes[c.center()].contains_key(&"poi/03/03".into()));
}
