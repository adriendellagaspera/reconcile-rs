use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::Entry;
use crate::persistence::PersistenceDelta;

use super::*;

fn sample_state() -> PersistedState<i32, String> {
    let mut members = HashSet::new();
    members.insert("127.0.0.1".parse().unwrap());
    members.insert("127.0.0.2".parse().unwrap());

    let mut acks = HashMap::new();
    let mut key_acks = HashMap::new();
    key_acks.insert("127.0.0.1".parse().unwrap(), 42u64);
    acks.insert(7, key_acks);

    PersistedState::new(
        vec![
            (
                1,
                Entry::present(
                    Timestamp::new(
                        Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::new(0)),
                        NodeId::new(7),
                    ),
                    "alive".to_string(),
                ),
            ),
            (
                2,
                Entry::tombstone(Timestamp::new(
                    Hlc::new(PhysicalTime::from_millis(2_000), LogicalCounter::new(1)),
                    NodeId::new(7),
                )),
            ), // tombstone
        ],
        members,
        acks,
    )
}

fn assert_states_eq(a: &PersistedState<i32, String>, b: &PersistedState<i32, String>) {
    assert_eq!(a.entries, b.entries);
    assert_eq!(a.members, b.members);
    assert_eq!(a.tombstone_acks, b.tombstone_acks);
}

#[test]
fn file_snapshot_save_then_load() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));

    // Nothing saved yet.
    assert!(Persistence::<i32, String>::load(&backend)
        .unwrap()
        .is_none());

    let state = sample_state();
    Persistence::<i32, String>::save(&backend, &state).unwrap();

    let loaded = Persistence::<i32, String>::load(&backend)
        .unwrap()
        .expect("a snapshot was saved");
    assert_states_eq(&loaded, &state);
}

fn assert_states_equivalent(a: &PersistedState<i32, String>, b: &PersistedState<i32, String>) {
    let a_entries: HashMap<_, _> = a.entries.iter().cloned().collect();
    let b_entries: HashMap<_, _> = b.entries.iter().cloned().collect();
    assert_eq!(a_entries, b_entries);
    assert_eq!(a.members, b.members);
    assert_eq!(a.tombstone_acks, b.tombstone_acks);
}

#[test]
fn delta_hook_requests_initial_base_then_handles_clean_noop() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));

    assert!(
        !Persistence::<i32, String>::try_save_delta(&backend, None).unwrap(),
        "an empty backend needs a materialized base"
    );
    let state = sample_state();
    Persistence::<i32, String>::save(&backend, &state).unwrap();
    let (store_dir, _) = incremental::paths(&backend);
    let before = fs::read_dir(&store_dir).unwrap().count();

    assert!(
        Persistence::<i32, String>::try_save_delta(&backend, None).unwrap(),
        "an existing base can treat a clean generation as a no-op"
    );
    assert_eq!(fs::read_dir(&store_dir).unwrap().count(), before);
}
#[test]
fn manifest_size_is_constant_as_delta_chain_grows() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let mut state = sample_state();
    for key in 100..356 {
        state.entries.push((
            key,
            Entry::present(
                Timestamp::new(
                    Hlc::new(
                        PhysicalTime::from_millis(10_000 + key as u64),
                        LogicalCounter::new(0),
                    ),
                    NodeId::new(7),
                ),
                format!("padding-{key:04}"),
            ),
        ));
    }
    Persistence::<i32, String>::save(&backend, &state).unwrap();

    let (_, manifest_path) = incremental::paths(&backend);
    let initial_len = fs::metadata(&manifest_path).unwrap().len();
    let delta = PersistenceDelta::<i32, String>::new(
        HashMap::new(),
        HashMap::new(),
        HashSet::new(),
        HashMap::new(),
    );

    for _ in 0..16 {
        assert!(Persistence::<i32, String>::try_save_delta(&backend, Some(&delta)).unwrap());
        assert_eq!(
            fs::metadata(&manifest_path).unwrap().len(),
            initial_len,
            "manifest encoding must not grow with segment count"
        );
    }

    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_equivalent(&loaded, &state);
}

#[test]
fn incremental_delta_replays_entries_members_and_acks() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let mut first = sample_state();
    for key in 100..132 {
        first.entries.push((
            key,
            Entry::present(
                Timestamp::new(
                    Hlc::new(
                        PhysicalTime::from_millis(10_000 + key as u64),
                        LogicalCounter::new(0),
                    ),
                    NodeId::new(7),
                ),
                "base-padding".repeat(8),
            ),
        ));
    }
    Persistence::<i32, String>::save(&backend, &first).unwrap();

    let peer2: std::net::IpAddr = "127.0.0.2".parse().unwrap();
    let peer3: std::net::IpAddr = "127.0.0.3".parse().unwrap();
    let updated = Entry::present(
        Timestamp::new(
            Hlc::new(PhysicalTime::from_millis(3_000), LogicalCounter::new(0)),
            NodeId::new(7),
        ),
        "updated".to_string(),
    );
    let added = Entry::present(
        Timestamp::new(
            Hlc::new(PhysicalTime::from_millis(4_000), LogicalCounter::new(0)),
            NodeId::new(7),
        ),
        "added".to_string(),
    );
    let mut expected = first.clone();
    expected.entries.retain(|(key, _)| *key != 1 && *key != 2);
    expected.entries.push((1, updated.clone()));
    expected.entries.push((3, added.clone()));
    expected.members.remove(&peer2);
    expected.members.insert(peer3);
    expected.tombstone_acks.remove(&7);
    expected
        .tombstone_acks
        .insert(9, HashMap::from([(peer3, 99)]));
    let delta = PersistenceDelta::new(
        HashMap::from([(1, Some(updated)), (2, None), (3, Some(added))]),
        HashMap::from([(peer2, false), (peer3, true)]),
        HashSet::from([7]),
        HashMap::from([(9, HashMap::from([(peer3, Some(99))]))]),
    );

    assert!(Persistence::<i32, String>::try_save_delta(&backend, Some(&delta)).unwrap());
    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_equivalent(&loaded, &expected);
}

#[test]
fn committed_missing_or_corrupt_delta_is_rejected() {
    for corrupt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
        let first = sample_state();
        Persistence::<i32, String>::save(&backend, &first).unwrap();

        let mut expected = first.clone();
        let added = Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(5_000), LogicalCounter::new(0)),
                NodeId::new(7),
            ),
            "delta".to_string(),
        );
        expected.entries.push((3, added.clone()));
        let delta = PersistenceDelta::new(
            HashMap::from([(3, Some(added))]),
            HashMap::new(),
            HashSet::new(),
            HashMap::new(),
        );
        assert!(Persistence::<i32, String>::try_save_delta(&backend, Some(&delta)).unwrap());

        let (store_dir, _) = incremental::paths(&backend);
        let delta_path = fs::read_dir(&store_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("delta-"))
            })
            .unwrap();
        if corrupt {
            let mut bytes = fs::read(&delta_path).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            fs::write(&delta_path, bytes).unwrap();
        } else {
            fs::remove_file(&delta_path).unwrap();
        }

        let err = Persistence::<i32, String>::load(&backend).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}

#[test]
fn corrupt_manifest_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let state = sample_state();
    Persistence::<i32, String>::save(&backend, &state).unwrap();

    let (_, manifest_path) = incremental::paths(&backend);
    let mut bytes = fs::read(&manifest_path).unwrap();
    // Manifest header: 4-byte magic plus 4-byte format version.
    let body_index = 8.min(bytes.len() - 1);
    bytes[body_index] ^= 0xff;
    fs::write(&manifest_path, bytes).unwrap();

    let err = Persistence::<i32, String>::load(&backend).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("checksum"));
}

#[test]
fn uncommitted_orphan_segment_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let state = sample_state();
    Persistence::<i32, String>::save(&backend, &state).unwrap();

    let (store_dir, _) = incremental::paths(&backend);
    fs::write(store_dir.join("delta-99999999999999999999.bin"), b"orphan").unwrap();

    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_eq(&loaded, &state);
}

#[test]
fn full_materialization_cleans_superseded_segments_after_publication() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let state = sample_state();
    Persistence::<i32, String>::save(&backend, &state).unwrap();

    let delta = PersistenceDelta::<i32, String>::new(
        HashMap::new(),
        HashMap::new(),
        HashSet::new(),
        HashMap::new(),
    );
    assert!(Persistence::<i32, String>::try_save_delta(&backend, Some(&delta)).unwrap());
    assert!(Persistence::<i32, String>::try_save_delta(&backend, Some(&delta)).unwrap());

    let (store_dir, _) = incremental::paths(&backend);
    fs::write(store_dir.join("delta-99999999999999999999.bin"), b"orphan").unwrap();
    let mut replacement = state.clone();
    replacement.entries.push((
        999,
        Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(99_000), LogicalCounter::new(0)),
                NodeId::new(7),
            ),
            "replacement-after-materialization".to_string(),
        ),
    ));
    Persistence::<i32, String>::save(&backend, &replacement).unwrap();

    let mut bases = 0;
    let mut deltas = 0;
    for entry in fs::read_dir(&store_dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if name.starts_with("base-") {
            bases += 1;
        } else if name.starts_with("delta-") {
            deltas += 1;
        }
    }
    assert_eq!(bases, 1, "only the newly committed base may remain");
    assert_eq!(deltas, 0, "all superseded/orphan deltas must be retired");
    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_equivalent(&loaded, &replacement);
}

#[test]
fn single_file_snapshot_layout_is_rejected_for_both_identity_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let old_bytes = b"obsolete single-file snapshot".to_vec();
    fs::write(&path, &old_bytes).unwrap();
    let backend = FileSnapshot::new(&path);

    let ip_state = sample_state();
    let signed_state = PersistedState::<i32, String, crate::PeerId>::default();

    assert_eq!(
        Persistence::<i32, String>::load(&backend)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<i32, String, crate::PeerId>::load(&backend)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<i32, String>::save(&backend, &ip_state)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<i32, String, crate::PeerId>::save(&backend, &signed_state)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<i32, String>::try_save_delta(&backend, None)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<i32, String, crate::PeerId>::try_save_delta(&backend, None)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    assert_eq!(fs::read(&path).unwrap(), old_bytes);
    assert!(!incremental::paths(&backend).1.exists());
}
