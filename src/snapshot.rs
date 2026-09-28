// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The file-backed [`Persistence`] adapter for a replicated map.
//! [`FileSnapshot`] stores an immutable materialized base plus ordered delta segments behind one
//! atomically-published manifest. Loading still accepts the previous single-file snapshot format
//! for migration. The port, snapshot value type and non-durable default live in
//! [`crate::persistence`]; this module owns the filesystem and codec adapter.
//! One type with no standalone reuse value outside this workspace, so it stays folded into
//! `reconcile` rather than earning its own crate. [`FileSnapshot`] is
//! re-exported from [`crate::persistence`] and from the crate root.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::persistence::{PersistedState, Persistence, PersistenceDelta};

mod incremental;

/// On-disk snapshot header: a 4-byte magic then a little-endian `u32` format version.
/// The body is bincode, not self-describing, so without this a format change would be silently
/// misread into a plausible-but-wrong state — dropping tombstones and re-enabling resurrection.
/// A pre-header snapshot is rejected the same way.
const SNAPSHOT_MAGIC: [u8; 4] = *b"RCNL";
/// Current on-disk snapshot format version. Bump whenever the serialized shape of
/// [`PersistedState`] changes. Version 1 is the `Entry<Timestamp, V>` / `State<V>` layout.
const SNAPSHOT_FORMAT_VERSION: u32 = 1;
/// Length of the header written ahead of the bincode body: magic (4) + version (4).
const SNAPSHOT_HEADER_LEN: usize = 8;

/// Validate the header, then decode the body. Every failure — short, wrong magic, unsupported
/// version, undecodable body — becomes an `InvalidData` error rather than a silent misread.
fn decode_snapshot<K, V>(bytes: &[u8]) -> io::Result<PersistedState<K, V>>
where
    K: DeserializeOwned + Eq + std::hash::Hash,
    V: DeserializeOwned,
{
    if bytes.len() < SNAPSHOT_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot is {} bytes, shorter than the {SNAPSHOT_HEADER_LEN}-byte format header \
                 (truncated, or a pre-Entry/State snapshot without the versioned header)",
                bytes.len()
            ),
        ));
    }
    let (header, body) = bytes.split_at(SNAPSHOT_HEADER_LEN);
    if header[..4] != SNAPSHOT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot magic {:02x?} does not match {SNAPSHOT_MAGIC:02x?}; the file is not a \
                 reconcile snapshot, or predates the versioned format",
                &header[..4]
            ),
        ));
    }
    let version = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot format version {version} is not supported by this build (expected \
                 {SNAPSHOT_FORMAT_VERSION}); it was written by a different reconcile version and \
                 must be migrated or discarded"
            ),
        ));
    }
    bincode::deserialize::<PersistedState<K, V>>(body)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

/// A durable, file-based [`Persistence`] backend holding one bincode-encoded snapshot.
/// Saves are **atomic**: written to a sibling `*.tmp`, flushed, then renamed over the target, then
/// the containing directory is synced (best-effort — some filesystems do not support syncing a
/// directory handle) so the rename itself survives a crash, not only the file's bytes.
#[derive(Clone, Debug)]
pub struct FileSnapshot {
    path: PathBuf,
}

impl FileSnapshot {
    /// Create a backend that reads from and writes to `path`.
    pub fn new(path: impl AsRef<Path>) -> Self {
        FileSnapshot {
            path: path.as_ref().to_path_buf(),
        }
    }

}

impl<K, V> Persistence<K, V> for FileSnapshot
where
    K: Serialize + DeserializeOwned + Eq + std::hash::Hash + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn load(&self) -> io::Result<Option<PersistedState<K, V>>> {
        if let Some(state) = incremental::load(self)? {
            return Ok(Some(state));
        }
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        decode_snapshot(&bytes).map(Some)
    }

    fn save(&self, state: &PersistedState<K, V>) -> io::Result<()> {
        incremental::save_full(self, state)
    }

    fn save_generation(
        &self,
        state: &PersistedState<K, V>,
        delta: Option<&PersistenceDelta<K, V>>,
    ) -> io::Result<()> {
        incremental::save_generation(self, state, delta)
    }
}

#[cfg(test)]
mod tests {
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

    fn assert_states_equivalent(
        a: &PersistedState<i32, String>,
        b: &PersistedState<i32, String>,
    ) {
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
    fn incremental_delta_replays_entries_members_and_acks() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
        let first = sample_state();
        Persistence::<i32, String>::save_generation(&backend, &first, None).unwrap();

        let peer1 = "127.0.0.1".parse().unwrap();
        let peer2 = "127.0.0.2".parse().unwrap();
        let peer3 = "127.0.0.3".parse().unwrap();
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
        let expected = PersistedState::new(
            vec![(1, updated.clone()), (3, added.clone())],
            HashSet::from([peer1, peer3]),
            HashMap::from([(9, HashMap::from([(peer3, 99)]))]),
        );
        let delta = PersistenceDelta::new(
            HashMap::from([(1, Some(updated)), (2, None), (3, Some(added))]),
            HashMap::from([(peer2, false), (peer3, true)]),
            HashSet::from([7]),
            HashMap::from([(9, HashMap::from([(peer3, Some(99))]))]),
        );

        Persistence::<i32, String>::save_generation(&backend, &expected, Some(&delta)).unwrap();
        let loaded = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
        assert_states_equivalent(&loaded, &expected);
    }

    #[test]
    fn committed_missing_or_corrupt_delta_is_rejected() {
        for corrupt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
            let first = sample_state();
            Persistence::<i32, String>::save_generation(&backend, &first, None).unwrap();

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
            Persistence::<i32, String>::save_generation(&backend, &expected, Some(&delta)).unwrap();

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
    fn uncommitted_orphan_segment_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
        let state = sample_state();
        Persistence::<i32, String>::save_generation(&backend, &state, None).unwrap();

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

        Persistence::<i32, String>::save_generation(&backend, &state, None).unwrap();
        assert!(!path.exists(), "legacy file should not shadow the committed manifest");
        let (_, manifest) = incremental::paths(&backend);
        assert!(manifest.exists());

        let migrated = Persistence::<i32, String>::load(&backend).unwrap().unwrap();
        assert_states_eq(&migrated, &state);
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
}
