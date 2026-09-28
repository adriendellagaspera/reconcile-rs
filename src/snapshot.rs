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
mod tests;
