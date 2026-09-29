use std::collections::{HashMap, HashSet};

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
fn persisted_state_bincode_roundtrip() {
    let state = sample_state();
    let bytes = bincode::serialize(&state).unwrap();
    let back: PersistedState<i32, String> = bincode::deserialize(&bytes).unwrap();
    assert_states_eq(&back, &state);
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

fn legacy_bytes(state: &PersistedState<i32, String>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&bincode::serialize(state).unwrap());
    bytes
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
    let state = sample_state();
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

    let peer1: std::net::IpAddr = "127.0.0.1".parse().unwrap();
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
    let body_index = SNAPSHOT_HEADER_LEN.min(bytes.len() - 1);
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
fn legacy_snapshot_migrates_on_next_generation_save() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let state = sample_state();
    fs::write(&path, legacy_bytes(&state)).unwrap();
    let backend = FileSnapshot::new(&path);

    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_eq(&loaded, &state);

    Persistence::<i32, String>::save(&backend, &state).unwrap();
    assert!(
        !path.exists(),
        "successful base publication must retire the legacy single-file snapshot"
    );
    let (_, manifest) = incremental::paths(&backend);
    assert!(manifest.exists());

    let migrated = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_eq(&migrated, &state);
}

#[test]
fn full_materialization_cleans_superseded_segments_after_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let backend = FileSnapshot::new(&path);
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
    fs::write(&path, b"legacy-or-stale").unwrap();

    Persistence::<i32, String>::save(&backend, &state).unwrap();

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
    assert!(
        !path.exists(),
        "legacy path must be retired after publication"
    );

    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_states_equivalent(&loaded, &state);
}

#[test]
fn file_snapshot_save_is_atomic_replace() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));

    let mut first = sample_state();
    first.entries = vec![(
        1,
        Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(1), LogicalCounter::new(0)),
                NodeId::new(0),
            ),
            "first".to_string(),
        ),
    )];
    Persistence::<i32, String>::save(&backend, &first).unwrap();

    let mut second = sample_state();
    second.entries = vec![(
        1,
        Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(2), LogicalCounter::new(0)),
                NodeId::new(0),
            ),
            "second".to_string(),
        ),
    )];
    Persistence::<i32, String>::save(&backend, &second).unwrap();

    // No leftover temporary file, and the latest snapshot wins.
    assert!(!dir.path().join("snapshot.bin.tmp").exists());
    let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
    assert_eq!(loaded.entries[0].1.value(), Some(&"second".to_string()));
}

/// A pre-header snapshot — valid bincode, no magic/version prefix — must be rejected as
/// `InvalidData`, never silently misread.
#[test]
fn headerless_legacy_snapshot_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    // Raw bincode body with no header prefix — the on-disk shape.
    let body = bincode::serialize(&sample_state()).unwrap();
    fs::write(&path, &body).unwrap();
    let backend = FileSnapshot::new(&path);
    let err = Persistence::<i32, String>::load(&backend).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

/// A snapshot carrying the right magic but a **future/unknown format version** must be rejected
/// rather than decoded with this build's layout.
#[test]
fn unknown_format_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&(SNAPSHOT_FORMAT_VERSION + 1).to_le_bytes());
    bytes.extend_from_slice(&bincode::serialize(&sample_state()).unwrap());
    fs::write(&path, &bytes).unwrap();
    let backend = FileSnapshot::new(&path);
    let err = Persistence::<i32, String>::load(&backend).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("format version"),
        "error should name the version mismatch, got: {err}"
    );
}

/// A snapshot truncated below the header length must be rejected, not panic on the
/// out-of-bounds header slice.
#[test]
fn truncated_snapshot_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.bin");
    fs::write(&path, [0xAB; 3]).unwrap();
    let backend = FileSnapshot::new(&path);
    let err = Persistence::<i32, String>::load(&backend).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
