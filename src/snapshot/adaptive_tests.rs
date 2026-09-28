use std::fs;
use std::sync::Arc;

use crate::replicated_map::Config;
use crate::{FileSnapshot, InMemoryNetwork, NodeId, ReplicatedMap};

use super::incremental;

fn store(
    network: &InMemoryNetwork,
    path: &std::path::Path,
    octet: u8,
) -> ReplicatedMap<u32, Vec<u8>> {
    let addr = format!("127.0.0.{octet}:8308").parse().unwrap();
    let transport = Arc::new(network.bind(addr));
    ReplicatedMap::new_with_transport(
        Config::new(8308)
            .with_listen_addr(addr.ip())
            .with_node_id(NodeId::new(u64::from(octet)))
            .with_snapshot_interval(None)
            .with_insecure_no_key(),
        transport,
    )
    .unwrap()
    .with_persistence(Arc::new(FileSnapshot::new(path)))
    .unwrap()
}

fn segment_counts(path: &std::path::Path) -> (usize, usize) {
    let backend = FileSnapshot::new(path);
    let (dir, _) = incremental::paths(&backend);
    let mut bases = 0;
    let mut deltas = 0;
    for entry in fs::read_dir(dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if name.starts_with("base-") {
            bases += 1;
        } else if name.starts_with("delta-") {
            deltas += 1;
        }
    }
    (bases, deltas)
}

#[test]
fn runtime_compacts_before_publishing_a_33rd_delta() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let network = InMemoryNetwork::new();
    let store = store(&network, &path, 10);

    let initial: Vec<_> = (0..1_000u32).map(|key| (key, vec![0; 64])).collect();
    store.load_bulk(&initial);
    store.snapshot_now().unwrap();

    for generation in 1..=32u8 {
        store.load_bulk(&[(0, vec![generation; 64])]);
        store.snapshot_now().unwrap();
    }
    assert_eq!(segment_counts(&path), (1, 32));

    let expected = store.fingerprint(..);
    store.load_bulk(&[(0, vec![33; 64])]);
    store.snapshot_now().unwrap();

    assert_eq!(
        segment_counts(&path),
        (1, 0),
        "the 33rd generation must materialize a fresh base and clean the old chain"
    );

    let restart_network = InMemoryNetwork::new();
    let restarted = store(&restart_network, &path, 11);
    assert_eq!(restarted.fingerprint(..), expected_after_write(expected, &store));
}

fn expected_after_write(
    _before: crate::Fingerprint,
    store: &ReplicatedMap<u32, Vec<u8>>,
) -> crate::Fingerprint {
    store.fingerprint(..)
}
