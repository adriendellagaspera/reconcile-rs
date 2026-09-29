// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::Hash;
use std::io;
use std::net::IpAddr;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::entry::Entry;
use crate::persistence::{PersistedState, PersistenceDelta};

use super::FileSnapshot;

mod storage;

use storage::{
    decode, invalid, publish_manifest, read_manifest, read_segment_bytes, store_dir, write_segment,
};

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
struct Manifest {
    base_generation: u64,
    base_bytes: u64,
    current_generation: u64,
    delta_bytes: u64,
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
    entries: &mut HashMap<K, Entry<Timestamp, V>>,
    members: &mut HashSet<IpAddr>,
    tombstone_acks: &mut HashMap<K, HashMap<IpAddr, u64>>,
    delta: PersistenceDelta<K, V>,
) where
    K: Eq + Hash,
{
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

    for (peer, present) in delta.members {
        if present {
            members.insert(peer);
        } else {
            members.remove(&peer);
        }
    }

    for key in delta.ack_key_clears {
        tombstone_acks.remove(&key);
    }
    for (key, peer_ops) in delta.ack_peers {
        use std::collections::hash_map::Entry as MapEntry;
        match tombstone_acks.entry(key) {
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

    let base_bytes = read_segment_bytes(
        &dir,
        "base",
        manifest.base_generation,
        Some(manifest.base_bytes),
    )?;
    let base: BaseSegment<K, V> = decode(&base_bytes, BASE_MAGIC)?;
    if base.generation != manifest.base_generation {
        return Err(invalid(format!(
            "base segment generation {} does not match manifest generation {}",
            base.generation, manifest.base_generation
        )));
    }

    let PersistedState {
        entries: base_entries,
        mut members,
        mut tombstone_acks,
    } = base.state;
    let mut entries: HashMap<K, Entry<Timestamp, V>> = base_entries.into_iter().collect();
    let mut observed_delta_bytes = 0u64;

    for generation in manifest.base_generation.saturating_add(1)..=manifest.current_generation {
        let bytes = read_segment_bytes(&dir, "delta", generation, None)?;
        observed_delta_bytes = observed_delta_bytes.saturating_add(bytes.len() as u64);
        let segment: DeltaSegment<K, V> = decode(&bytes, DELTA_MAGIC)?;
        let expected_from = generation.saturating_sub(1);
        if segment.from_generation != expected_from || segment.to_generation != generation {
            return Err(invalid(format!(
                "delta continuity error: expected {expected_from}..{generation}, got {}..{}",
                segment.from_generation, segment.to_generation
            )));
        }
        apply_delta(
            &mut entries,
            &mut members,
            &mut tombstone_acks,
            segment.delta,
        );
    }

    if observed_delta_bytes != manifest.delta_bytes {
        return Err(invalid(format!(
            "manifest records {} committed delta bytes, recovered {observed_delta_bytes}",
            manifest.delta_bytes
        )));
    }

    Ok(Some(PersistedState::new(
        entries.into_iter().collect(),
        members,
        tombstone_acks,
    )))
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
    let base_bytes = write_segment(&dir, BASE_MAGIC, "base", generation, &base)?;
    let manifest = Manifest {
        base_generation: generation,
        base_bytes,
        current_generation: generation,
        delta_bytes: 0,
    };
    publish_manifest(backend, &manifest)?;
    Ok(())
}

pub(super) fn try_save_delta<K, V>(
    backend: &FileSnapshot,
    delta: Option<&PersistenceDelta<K, V>>,
) -> io::Result<bool>
where
    K: Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    let Some(mut manifest) = read_manifest(backend)? else {
        return Ok(false);
    };
    let Some(delta) = delta else {
        return Ok(true);
    };

    let from_generation = manifest.current_generation;
    let to_generation = from_generation.saturating_add(1);
    let segment = DeltaSegmentWrite {
        from_generation,
        to_generation,
        delta,
    };
    let dir = store_dir(backend);
    let bytes = write_segment(&dir, DELTA_MAGIC, "delta", to_generation, &segment)?;
    manifest.current_generation = to_generation;
    manifest.delta_bytes = manifest.delta_bytes.saturating_add(bytes);
    publish_manifest(backend, &manifest)?;
    Ok(true)
}

#[cfg(test)]
mod tests;
