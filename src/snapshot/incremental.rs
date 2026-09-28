// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::fs;
use std::hash::Hash;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::entry::Entry;
use crate::persistence::{PersistedState, PersistenceDelta};

use super::FileSnapshot;

const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 8;
const BASE_MAGIC: [u8; 4] = *b"RCNB";
const DELTA_MAGIC: [u8; 4] = *b"RCND";
const MANIFEST_MAGIC: [u8; 4] = *b"RCNM";
const MANIFEST_FILE: &str = "manifest";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SegmentRef {
    generation: u64,
    file: String,
    checksum: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Manifest {
    base: SegmentRef,
    deltas: Vec<SegmentRef>,
    current_generation: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Eq + Hash, V: Deserialize<'de>"
))]
struct BaseSegment<K, V> {
    generation: u64,
    state: PersistedState<K, V>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Eq + Hash, V: Deserialize<'de>"
))]
struct DeltaSegment<K, V> {
    from_generation: u64,
    to_generation: u64,
    delta: PersistenceDelta<K, V>,
}

#[derive(Serialize)]
struct DeltaSegmentWrite<'a, K, V> {
    from_generation: u64,
    to_generation: u64,
    delta: &'a PersistenceDelta<K, V>,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn store_dir(backend: &FileSnapshot) -> PathBuf {
    append_suffix(&backend.path, ".d")
}

fn manifest_path(backend: &FileSnapshot) -> PathBuf {
    store_dir(backend).join(MANIFEST_FILE)
}

fn encode<T: Serialize>(magic: [u8; 4], value: &T) -> io::Result<Vec<u8>> {
    let body =
        bincode::serialize(value).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

fn decode<T: DeserializeOwned>(bytes: &[u8], expected_magic: [u8; 4]) -> io::Result<T> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid(format!(
            "incremental snapshot object is {} bytes, shorter than its {HEADER_LEN}-byte header",
            bytes.len()
        )));
    }
    if bytes[..4] != expected_magic {
        return Err(invalid(format!(
            "incremental snapshot magic {:02x?} does not match {:02x?}",
            &bytes[..4],
            expected_magic
        )));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != FORMAT_VERSION {
        return Err(invalid(format!(
            "incremental snapshot format version {version} is unsupported (expected {FORMAT_VERSION})"
        )));
    }
    bincode::deserialize(&bytes[HEADER_LEN..])
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn checksum(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn sync_dir(path: &Path) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    fs::create_dir_all(parent)?;
    let tmp = append_suffix(path, ".tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(parent);
    Ok(())
}

fn segment_file_name(kind: &str, generation: u64) -> String {
    format!("{kind}-{generation:020}.bin")
}

fn validate_segment_name(name: &str) -> io::Result<()> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(invalid(format!(
            "incremental snapshot manifest contains invalid segment path {name:?}"
        ))),
    }
}

fn read_manifest(backend: &FileSnapshot) -> io::Result<Option<Manifest>> {
    let path = manifest_path(backend);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let manifest: Manifest = decode(&bytes, MANIFEST_MAGIC)?;
    validate_manifest(&manifest)?;
    Ok(Some(manifest))
}

fn validate_manifest(manifest: &Manifest) -> io::Result<()> {
    validate_segment_name(&manifest.base.file)?;
    if manifest.base.generation > manifest.current_generation {
        return Err(invalid("manifest base generation is newer than current generation"));
    }

    let mut expected = manifest.base.generation.saturating_add(1);
    for delta in &manifest.deltas {
        validate_segment_name(&delta.file)?;
        if delta.generation != expected {
            return Err(invalid(format!(
                "manifest delta continuity error: expected generation {expected}, got {}",
                delta.generation
            )));
        }
        expected = expected.saturating_add(1);
    }
    let recovered = manifest
        .deltas
        .last()
        .map_or(manifest.base.generation, |delta| delta.generation);
    if recovered != manifest.current_generation {
        return Err(invalid(format!(
            "manifest current generation {} does not match referenced generation {recovered}",
            manifest.current_generation
        )));
    }
    Ok(())
}

fn read_segment_bytes(dir: &Path, reference: &SegmentRef) -> io::Result<Vec<u8>> {
    validate_segment_name(&reference.file)?;
    let path = dir.join(&reference.file);
    let bytes = fs::read(&path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            invalid(format!(
                "committed incremental snapshot segment {:?} is missing",
                reference.file
            ))
        } else {
            err
        }
    })?;
    let actual = checksum(&bytes);
    if actual != reference.checksum {
        return Err(invalid(format!(
            "incremental snapshot segment {:?} failed checksum validation",
            reference.file
        )));
    }
    Ok(bytes)
}

fn apply_delta<K, V>(
    mut state: PersistedState<K, V>,
    delta: PersistenceDelta<K, V>,
) -> PersistedState<K, V>
where
    K: Eq + Hash,
{
    let mut entries: HashMap<K, Entry<Timestamp, V>> = state.entries.into_iter().collect();
    for (key, entry) in delta.entries {
        match entry {
            Some(entry) => {
                entries.insert(key, entry);
            }
            None => {
                entries.remove(&key);
            }
        }
    }
    state.entries = entries.into_iter().collect();

    for (peer, present) in delta.members {
        if present {
            state.members.insert(peer);
        } else {
            state.members.remove(&peer);
        }
    }

    for key in delta.ack_key_clears {
        state.tombstone_acks.remove(&key);
    }
    for (key, peer_ops) in delta.ack_peers {
        use std::collections::hash_map::Entry as MapEntry;
        match state.tombstone_acks.entry(key) {
            MapEntry::Occupied(mut occupied) => {
                for (peer, version) in peer_ops {
                    match version {
                        Some(version) => {
                            occupied.get_mut().insert(peer, version);
                        }
                        None => {
                            occupied.get_mut().remove(&peer);
                        }
                    }
                }
                if occupied.get().is_empty() {
                    occupied.remove();
                }
            }
            MapEntry::Vacant(vacant) => {
                let acks: HashMap<_, _> = peer_ops
                    .into_iter()
                    .filter_map(|(peer, version)| version.map(|version| (peer, version)))
                    .collect();
                if !acks.is_empty() {
                    vacant.insert(acks);
                }
            }
        }
    }

    state
}

pub(super) fn load<K, V>(backend: &FileSnapshot) -> io::Result<Option<PersistedState<K, V>>>
where
    K: DeserializeOwned + Eq + Hash,
    V: DeserializeOwned,
{
    let Some(manifest) = read_manifest(backend)? else {
        return Ok(None);
    };
    let dir = store_dir(backend);

    let base_bytes = read_segment_bytes(&dir, &manifest.base)?;
    let base: BaseSegment<K, V> = decode(&base_bytes, BASE_MAGIC)?;
    if base.generation != manifest.base.generation {
        return Err(invalid(format!(
            "base segment generation {} does not match manifest generation {}",
            base.generation, manifest.base.generation
        )));
    }

    let mut state = base.state;
    let mut current = base.generation;
    for reference in &manifest.deltas {
        let bytes = read_segment_bytes(&dir, reference)?;
        let segment: DeltaSegment<K, V> = decode(&bytes, DELTA_MAGIC)?;
        if segment.from_generation != current
            || segment.to_generation != reference.generation
            || segment.to_generation != current.saturating_add(1)
        {
            return Err(invalid(format!(
                "delta continuity error: current={current}, segment={}..{}, manifest={}",
                segment.from_generation, segment.to_generation, reference.generation
            )));
        }
        state = apply_delta(state, segment.delta);
        current = segment.to_generation;
    }
    if current != manifest.current_generation {
        return Err(invalid(format!(
            "recovered generation {current} does not match manifest generation {}",
            manifest.current_generation
        )));
    }
    Ok(Some(state))
}

fn write_segment<T: Serialize>(
    dir: &Path,
    magic: [u8; 4],
    kind: &str,
    generation: u64,
    value: &T,
) -> io::Result<SegmentRef> {
    let bytes = encode(magic, value)?;
    let file = segment_file_name(kind, generation);
    write_atomic(&dir.join(&file), &bytes)?;
    Ok(SegmentRef {
        generation,
        file,
        checksum: checksum(&bytes),
    })
}

fn publish_manifest(backend: &FileSnapshot, manifest: &Manifest) -> io::Result<()> {
    validate_manifest(manifest)?;
    let bytes = encode(MANIFEST_MAGIC, manifest)?;
    write_atomic(&manifest_path(backend), &bytes)
}

fn remove_legacy_after_migration(backend: &FileSnapshot) {
    match fs::remove_file(&backend.path) {
        Ok(()) => {
            if let Some(parent) = backend.path.parent().filter(|path| !path.as_os_str().is_empty()) {
                sync_dir(parent);
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

fn cleanup_replaced_segments(dir: &Path, previous: Option<&Manifest>, keep: &Manifest) {
    let Some(previous) = previous else {
        return;
    };
    let mut keep_files = std::collections::HashSet::new();
    keep_files.insert(keep.base.file.as_str());
    for delta in &keep.deltas {
        keep_files.insert(delta.file.as_str());
    }

    let refs = std::iter::once(&previous.base).chain(previous.deltas.iter());
    for reference in refs {
        if !keep_files.contains(reference.file.as_str()) {
            let _ = fs::remove_file(dir.join(&reference.file));
        }
    }
    sync_dir(dir);
}

pub(super) fn save_full<K, V>(
    backend: &FileSnapshot,
    state: &PersistedState<K, V>,
) -> io::Result<()>
where
    K: Serialize,
    V: Serialize,
{
    let previous = read_manifest(backend)?;
    let generation = previous
        .as_ref()
        .map_or(1, |manifest| manifest.current_generation.saturating_add(1));
    let dir = store_dir(backend);
    fs::create_dir_all(&dir)?;

    let base = BaseSegment { generation, state };
    let base_ref = write_segment(&dir, BASE_MAGIC, "base", generation, &base)?;
    let manifest = Manifest {
        base: base_ref,
        deltas: Vec::new(),
        current_generation: generation,
    };
    publish_manifest(backend, &manifest)?;
    remove_legacy_after_migration(backend);
    cleanup_replaced_segments(&dir, previous.as_ref(), &manifest);
    Ok(())
}

pub(super) fn save_generation<K, V>(
    backend: &FileSnapshot,
    full_state: &PersistedState<K, V>,
    delta: Option<&PersistenceDelta<K, V>>,
) -> io::Result<()>
where
    K: Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    let Some(mut manifest) = read_manifest(backend)? else {
        return save_full(backend, full_state);
    };
    let Some(delta) = delta else {
        return Ok(());
    };

    let from_generation = manifest.current_generation;
    let to_generation = from_generation.saturating_add(1);
    let segment = DeltaSegmentWrite {
        from_generation,
        to_generation,
        delta,
    };
    let dir = store_dir(backend);
    let reference = write_segment(&dir, DELTA_MAGIC, "delta", to_generation, &segment)?;
    manifest.deltas.push(reference);
    manifest.current_generation = to_generation;
    publish_manifest(backend, &manifest)?;
    Ok(())
}

#[cfg(test)]
pub(super) fn paths(backend: &FileSnapshot) -> (PathBuf, PathBuf) {
    (store_dir(backend), manifest_path(backend))
}
