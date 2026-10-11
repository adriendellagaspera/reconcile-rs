use super::*;

async fn wait(cluster: &Cluster, predicate: impl Fn(&serde_json::Value) -> bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if predicate(&cluster.state()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("real protocol did not reach expected state");
}

#[tokio::test(flavor = "current_thread")]
async fn partition_keeps_local_writes_and_only_the_changed_range_diverges() {
    let mut cluster = Cluster::new().unwrap();
    cluster.next().unwrap();
    wait(&cluster, |s| s["equal"] == true).await;
    assert_eq!(
        cluster.state()["nodes"][2]["entries"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    cluster.next().unwrap();
    assert_eq!(
        cluster.nodes[0].get_cloned(&KEYS[1].into()).as_deref(),
        Some("alpha")
    );
    assert_eq!(
        cluster.nodes[2].get_cloned(&KEYS[1].into()).as_deref(),
        Some("charlie")
    );
    wait(&cluster, |s| s["ready"] == true).await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(
        cluster.nodes[1].get_cloned(&KEYS[1].into()).as_deref(),
        Some("alpha")
    );
    assert_eq!(
        cluster.nodes[2].get_cloned(&KEYS[1].into()).as_deref(),
        Some("charlie")
    );
    let s = cluster.state();
    for i in [0, 2] {
        assert_eq!(s["nodes"][0]["ranges"][i], s["nodes"][2]["ranges"][i]);
    }
    assert_ne!(s["nodes"][0]["ranges"][1], s["nodes"][2]["ranges"][1]);
    assert!(s["dropped"].as_u64().unwrap() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_converges_to_the_greatest_stamp_without_an_application_rewrite() {
    let mut cluster = Cluster::new().unwrap();
    cluster.next().unwrap();
    wait(&cluster, |s| s["equal"] == true).await;
    cluster.next().unwrap();
    let a = cluster.nodes[0].snapshot().get(KEYS[1]).unwrap().clone();
    let c = cluster.nodes[2].snapshot().get(KEYS[1]).unwrap().clone();
    let winner = if a.stamp > c.stamp { a } else { c };
    wait(&cluster, |s| s["ready"] == true).await;
    cluster.next().unwrap();
    wait(&cluster, |s| s["equal"] == true).await;
    for node in &cluster.nodes {
        assert_eq!(node.snapshot().get(KEYS[1]), Some(&winner));
    }
    assert!(cluster.state()["links"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["datagrams"].as_u64().unwrap() > 0));
    assert!(cluster.next().is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn dated_equality_and_reset_do_not_depend_on_a_displayed_fingerprint() {
    let mut cluster = Cluster::new().unwrap();
    let pinned = cluster.nodes[0].snapshot();
    cluster.nodes[0].insert(KEYS[1].into(), "same".into());
    cluster.nodes[2].insert(KEYS[1].into(), "same".into());
    assert_ne!(
        cluster.nodes[0].snapshot().get(KEYS[1]).unwrap().stamp,
        cluster.nodes[2].snapshot().get(KEYS[1]).unwrap().stamp
    );
    assert!(pinned.is_empty());
    assert_eq!(cluster.state()["equal"], false);
    assert!(cluster.next().is_err());
    super::super::controls::apply(&mut cluster, "reset").unwrap();
    assert_eq!(cluster.stage, 0);
    assert!(cluster.nodes.iter().all(ReplicatedMap::is_empty));
    assert_eq!(cluster.state()["equal"], true);
    assert!(super::super::controls::apply(&mut cluster, "invalid").is_err());
}
