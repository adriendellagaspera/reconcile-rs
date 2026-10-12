use super::super::controls;
use super::*;
fn apply(c: &mut Cluster, path: &str) {
    controls::apply(c, path).unwrap();
}
async fn agree(c: &Cluster) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if c.state()["equal"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("protocol did not converge");
}
#[tokio::test(flavor = "current_thread")]
async fn every_replica_can_write_offline_and_overlapping_ranges_converge_by_stamp() {
    for id in 0..SIZE {
        let mut c = Cluster::new().unwrap();
        apply(&mut c, "write/0/0/64/736861726564");
        agree(&c).await;
        apply(&mut c, &format!("connect/{id}/0"));
        apply(&mut c, &format!("write/{id}/16/32/6c6f63616c"));
        let other = (id + 1) % SIZE;
        apply(&mut c, &format!("write/{other}/16/32/72656d6f7465"));
        let local = c.nodes[id].snapshot();
        let remote = c.nodes[other].snapshot();
        let winner = if local.get(&20).unwrap().stamp > remote.get(&20).unwrap().stamp {
            local.get(&20).unwrap().clone()
        } else {
            remote.get(&20).unwrap().clone()
        };
        tokio::time::sleep(Duration::from_millis(950)).await;
        assert_eq!(c.nodes[id].get_cloned(&20).as_deref(), Some("local"));
        apply(&mut c, &format!("connect/{id}/1"));
        agree(&c).await;
        for n in &c.nodes {
            assert_eq!(n.snapshot().get(&20), Some(&winner));
            assert_eq!(n.len(), 64);
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn delete_range_propagates_tombstones_without_deleting_neighboring_keys() {
    let mut c = Cluster::new().unwrap();
    apply(&mut c, "write/0/0/96/78");
    agree(&c).await;
    apply(&mut c, "connect/2/0");
    apply(&mut c, "delete/0/32/64");
    assert_eq!(c.nodes[0].len(), 64);
    assert_eq!(c.nodes[2].len(), 96);
    for key in 32..64 {
        assert!(c.nodes[0].snapshot().get(&key).unwrap().is_tombstone());
    }
    apply(&mut c, "heal");
    agree(&c).await;
    for n in &c.nodes {
        assert_eq!(n.len(), 64);
        assert_eq!(n.get_cloned(&31).as_deref(), Some("x"));
        assert_eq!(n.get_cloned(&64).as_deref(), Some("x"));
        assert_eq!(n.get_cloned(&32), None);
    }
    apply(&mut c, "write/1/40/41/7265626f726e");
    agree(&c).await;
    assert!(c
        .nodes
        .iter()
        .all(|n| n.get_cloned(&40).as_deref() == Some("reborn")));
}
#[tokio::test(flavor = "current_thread")]
async fn a_cut_link_can_reconcile_via_a_third_replica() {
    let mut c = Cluster::new().unwrap();
    apply(&mut c, "link/0/2/0");
    apply(&mut c, "write/0/0/8/78");
    agree(&c).await;
    assert_eq!(c.nodes[2].len(), 8);
    assert!(!c.network.allowed(0, 2));
    assert!(c.network.allowed(0, 1));
    assert!(c.network.allowed(1, 2));
    apply(&mut c, "connect/1/0");
    apply(&mut c, "write/0/0/1/79");
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(c.nodes[2].get_cloned(&0).as_deref(), Some("x"));
    apply(&mut c, "link/0/2/1");
    tokio::time::timeout(Duration::from_secs(12), async {
        while c.nodes[2].get_cloned(&0).as_deref() != Some("y") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(c.nodes[1].get_cloned(&0).as_deref(), Some("x"));
    assert_eq!(c.state()["equal"], false);
    apply(&mut c, "connect/1/1");
    agree(&c).await;
}
#[tokio::test(flavor = "current_thread")]
async fn validation_is_atomic_and_noop_delete_is_acknowledged() {
    let mut c = Cluster::new().unwrap();
    let snapshot = c.nodes[0].snapshot();
    for path in [
        "write/3/0/1/78",
        "write/0/1/0/78",
        "write/0/0/1025/78",
        "write/0/0/1/zz",
        "write/0/0/1/ff",
        "connect/0/2",
        "link/0/0/1",
        "next",
    ] {
        assert!(controls::apply(&mut c, path).is_err());
        assert_eq!(c.revision, 0);
        assert!(c.nodes.iter().all(ReplicatedMap::is_empty));
    }
    apply(&mut c, "delete/0/0/8");
    assert!(c.last_action.contains("deleted 0"));
    assert_eq!(c.revision, 1);
    apply(&mut c, "write/1/1023/1024/c3a9");
    assert_eq!(c.nodes[1].get_cloned(&1023).as_deref(), Some("é"));
    assert!(snapshot.is_empty());
    apply(&mut c, "reset");
    assert_eq!(c.revision, 3);
    assert!(c.nodes.iter().all(ReplicatedMap::is_empty));
}
