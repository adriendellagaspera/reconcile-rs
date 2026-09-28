// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{
    FileSnapshot, Manifest, SegmentRef, FORMAT_VERSION, HEADER_LEN, MANIFEST_FILE,
    MANIFEST_MAGIC,
};

pub(super) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(super) fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

pub(super) fn store_dir(backend: &FileSnapshot) -> PathBuf {
    append_suffix(&backend.path, ".d")
}

pub(super) fn manifest_path(backend: &FileSnapshot) -> PathBuf {
    store_dir(backend).join(MANIFEST_FILE)
}

pub(super) fn encode<T: Serialize>(magic: [u8; 4], value: &T) -> io::Result<Vec<u8>> {
    let body =
        bincode::serialize(value).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], expected_magic: [u8; 4]) -> io::Result<T> {
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

pub(super) fn checksum(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

pub(super) fn sync_dir(path: &Path) {
    if let Ok(dir) = fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

pub(super) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
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

pub(super) fn segment_file_name(kind: &str, generation: u64) -> String {
    format!("{kind}-{generation:020}.bin")
}

pub(super) fn validate_segment_name(name: &str) -> io::Result<()> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(invalid(format!(
            "incremental snapshot manifest contains invalid segment path {name:?}"
        ))),
    }
}

pub(super) fn read_manifest(backend: &FileSnapshot) -> io::Result<Option<Manifest>> {
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

pub(super) fn validate_manifest(manifest: &Manifest) -> io::Result<()> {
    validate_segment_name(&manifest.base.file)?;
    if manifest.base.generation > manifest.current_generation {
        return Err(invalid(
            "manifest base generation is newer than current generation",
        ));
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

pub(super) fn read_segment_bytes(dir: &Path, reference: &SegmentRef) -> io::Result<Vec<u8>> {
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

pub(super) fn write_segment<T: Serialize>(
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

pub(super) fn publish_manifest(backend: &FileSnapshot, manifest: &Manifest) -> io::Result<()> {
    validate_manifest(manifest)?;
    let bytes = encode(MANIFEST_MAGIC, manifest)?;
    write_atomic(&manifest_path(backend), &bytes)
}

pub(super) fn remove_legacy_after_migration(backend: &FileSnapshot) {
    match fs::remove_file(&backend.path) {
        Ok(()) => {
            if let Some(parent) = backend
                .path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                sync_dir(parent);
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

pub(super) fn cleanup_replaced_segments(dir: &Path, previous: Option<&Manifest>, keep: &Manifest) {
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

#[cfg(test)]
pub(super) fn paths(backend: &FileSnapshot) -> (PathBuf, PathBuf) {
    (store_dir(backend), manifest_path(backend))
}

