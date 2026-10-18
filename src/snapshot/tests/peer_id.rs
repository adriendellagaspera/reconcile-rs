// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::Entry;

use super::*;

type SignedState = PersistedState<u32, String, PeerId>;

fn signed_state() -> SignedState {
    let a = PeerId::from_bytes([0x11; 32]);
    let b = PeerId::from_bytes([0x22; 32]);
    let entries = (0..24)
        .map(|key| {
            (
                key,
                Entry::present(
                    Timestamp::new(
                        Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::new(0)),
                        NodeId::new(7),
                    ),
                    "value".repeat(160),
                ),
            )
        })
        .collect();
    SignedState::new(
        entries,
        HashSet::from([a, b]),
        HashMap::from([(9, HashMap::from([(a, 17)]))]),
    )
}

fn load_signed(backend: &FileSnapshot) -> io::Result<Option<SignedState>> {
    Persistence::<u32, String, PeerId>::load(backend)
}

#[test]
fn peer_id_base_and_delta_survive_restart_with_exact_ack_versions() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("store.bin"));
    let before = signed_state();
    Persistence::<u32, String, PeerId>::save(&backend, &before).unwrap();
    assert_eq!(
        load_signed(&backend).unwrap().unwrap().members,
        before.members
    );

    let a = PeerId::from_bytes([0x11; 32]);
    let b = PeerId::from_bytes([0x22; 32]);
    let c = PeerId::from_bytes([0x33; 32]);
    let delta = PersistenceDelta::new(
        HashMap::new(),
        HashMap::from([(b, false), (c, true)]),
        HashSet::new(),
        HashMap::from([(9, HashMap::from([(a, Some(17)), (c, Some(17))]))]),
    );
    assert!(Persistence::<u32, String, PeerId>::try_save_delta(&backend, Some(&delta)).unwrap());
    let after = load_signed(&backend).unwrap().unwrap();
    let recovered: HashMap<_, _> = after.entries.iter().cloned().collect();
    let original: HashMap<_, _> = before.entries.iter().cloned().collect();
    assert_eq!(recovered, original);
    assert_eq!(after.members, HashSet::from([a, c]));
    assert_eq!(after.tombstone_acks[&9][&a], 17);
    assert_eq!(after.tombstone_acks[&9][&c], 17);

    let (_, manifest) = incremental::paths(&backend);
    assert_eq!(&fs::read(manifest).unwrap()[..8], b"RCNM\x02\x00\x00\x00");
}

#[test]
fn empty_member_sets_do_not_allow_reading_a_snapshot_under_the_wrong_identity_schema() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("store.bin"));
    let empty_ip = PersistedState::<u32, String>::default();
    Persistence::<u32, String>::save(&backend, &empty_ip).unwrap();

    let error = load_signed(&backend).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("format version"));
    assert_eq!(
        Persistence::<u32, String, PeerId>::save(&backend, &signed_state())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    let peer_backend = FileSnapshot::new(dir.path().join("signed.bin"));
    Persistence::<u32, String, PeerId>::save(&peer_backend, &SignedState::default()).unwrap();
    let error = Persistence::<u32, String>::load(&peer_backend).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("format version"));
}

#[test]
fn peer_id_mode_rejects_old_single_file_on_read_and_before_any_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");
    let mut old_bytes = Vec::from(SNAPSHOT_MAGIC);
    old_bytes.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    old_bytes
        .extend_from_slice(&bincode::serialize(&PersistedState::<u32, String>::default()).unwrap());
    fs::write(&path, &old_bytes).unwrap();
    let backend = FileSnapshot::new(&path);

    assert_eq!(
        load_signed(&backend).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<u32, String, PeerId>::save(&backend, &signed_state())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        Persistence::<u32, String, PeerId>::try_save_delta(&backend, None)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fs::read(&path).unwrap(), old_bytes);
    assert!(!incremental::paths(&backend).1.exists());
}

#[test]
fn signed_single_file_v2_is_decodable_without_guessing_the_identity_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.bin");
    let state = signed_state();
    let mut bytes = Vec::from(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&incremental::PEER_ID_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&bincode::serialize(&state).unwrap());
    fs::write(&path, bytes).unwrap();
    let restored = load_signed(&FileSnapshot::new(path)).unwrap().unwrap();
    assert_eq!(restored.members, state.members);
    assert_eq!(restored.tombstone_acks, state.tombstone_acks);
}

#[test]
fn uncommitted_peer_id_delta_is_ignored_until_manifest_publication() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("store.bin"));
    let state = signed_state();
    Persistence::<u32, String, PeerId>::save(&backend, &state).unwrap();
    let (store, _) = incremental::paths(&backend);
    fs::write(store.join("delta-00000000000000000002.bin"), b"incomplete").unwrap();

    let recovered = load_signed(&backend).unwrap().unwrap();
    assert_eq!(recovered.members, state.members);
    assert_eq!(recovered.tombstone_acks, state.tombstone_acks);
}

#[test]
fn corrupt_committed_peer_id_delta_does_not_reset_causal_history() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("store.bin"));
    Persistence::<u32, String, PeerId>::save(&backend, &signed_state()).unwrap();
    let c = PeerId::from_bytes([0x33; 32]);
    let delta = PersistenceDelta::new(
        HashMap::new(),
        HashMap::from([(c, true)]),
        HashSet::new(),
        HashMap::from([(9, HashMap::from([(c, Some(17))]))]),
    );
    assert!(Persistence::<u32, String, PeerId>::try_save_delta(&backend, Some(&delta)).unwrap());
    let (store, _) = incremental::paths(&backend);
    let path = store.join("delta-00000000000000000002.bin");
    let mut bytes = fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(path, bytes).unwrap();

    let err = load_signed(&backend).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("checksum"));
}
