// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::fs;
use std::hash::Hash;
use std::io;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::entry::Entry;
use crate::persistence::{PersistedState, PersistenceDelta};

use super::FileSnapshot;

mod storage;

use storage::{decode, invalid, publish_manifest, read_manifest, read_segment_bytes, store_dir, write_segment};

#[cfg(test)]
pub(super) fn paths(backend: &FileSnapshot) -> (std::path::PathBuf, std::path::PathBuf) {
    storage::paths(backend)
}

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
    bytes: u64,
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

#[derive(Serialize)]
struct BaseSegmentWrite<'a, K, V> {
    generation: u64,
    state: &'a PersistedState<K, V>,
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
    for reference in &manifest.deltas {
        let bytes = read_segment_bytes(&dir, reference)?;
        let segment: DeltaSegment<K, V> = decode(&bytes, DELTA_MAGIC)?;
        let expected_from = reference.generation.saturating_sub(1);
        if segment.from_generation != expected_from
            || segment.to_generation != reference.generation
        {
            return Err(invalid(format!(
                "delta continuity error: expected {expected_from}..{}, got {}..{}",
                reference.generation, segment.from_generation, segment.to_generation
            )));
        }
        state = apply_delta(state, segment.delta);
    }
    Ok(Some(state))
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

    let base = BaseSegmentWrite { generation, state };
    let base_ref = write_segment(&dir, BASE_MAGIC, "base", generation, &base)?;
    let manifest = Manifest {
        base: base_ref,
        deltas: Vec::new(),
        current_generation: generation,
    };
    publish_manifest(backend, &manifest)?;
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
mod tests;
