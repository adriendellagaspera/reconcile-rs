// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::observability;

use super::{FileSnapshot, Manifest, FORMAT_VERSION, HEADER_LEN, MANIFEST_FILE, MANIFEST_MAGIC};

pub(super) const OBJECT_CHECKSUM_LEN: usize = 32;

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
    let digest = blake3::hash(&body);
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len() + OBJECT_CHECKSUM_LEN);
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(digest.as_bytes());
    Ok(bytes)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], expected_magic: [u8; 4]) -> io::Result<T> {
    if bytes.len() < HEADER_LEN + OBJECT_CHECKSUM_LEN {
        return Err(invalid(format!(
            "incremental snapshot object is {} bytes, shorter than its header plus checksum",
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
    let checksum_offset = bytes.len() - OBJECT_CHECKSUM_LEN;
    let body = &bytes[HEADER_LEN..checksum_offset];
    let expected = &bytes[checksum_offset..];
    let actual = blake3::hash(body);
    if actual.as_bytes() != expected {
        return Err(invalid(
            "incremental snapshot object failed checksum validation",
        ));
    }
    bincode::deserialize(body).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
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
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn segment_file_name(kind: &str, generation: u64) -> String {
    format!("{kind}-{generation:020}.bin")
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
    if manifest.base_generation == 0 || manifest.base_generation > manifest.current_generation {
        return Err(invalid(
            "manifest base generation must be nonzero and not newer than current generation",
        ));
    }
    if manifest.base_bytes == 0 {
        return Err(invalid(
            "manifest base segment must have a nonzero encoded size",
        ));
    }
    if manifest.base_generation == manifest.current_generation && manifest.delta_bytes != 0 {
        return Err(invalid(
            "manifest without committed deltas must record zero delta bytes",
        ));
    }
    if manifest.base_generation < manifest.current_generation && manifest.delta_bytes == 0 {
        return Err(invalid(
            "manifest with committed deltas must record nonzero delta bytes",
        ));
    }
    Ok(())
}

pub(super) fn read_segment_bytes(
    dir: &Path,
    kind: &str,
    generation: u64,
    expected_bytes: Option<u64>,
) -> io::Result<Vec<u8>> {
    let file = segment_file_name(kind, generation);
    let path = dir.join(&file);
    let bytes = fs::read(&path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            invalid(format!(
                "committed incremental snapshot segment {file:?} is missing"
            ))
        } else {
            err
        }
    })?;
    if let Some(expected) = expected_bytes {
        if bytes.len() as u64 != expected {
            return Err(invalid(format!(
                "incremental snapshot segment {file:?} has {} bytes, manifest records {expected}",
                bytes.len()
            )));
        }
    }
    Ok(bytes)
}

pub(super) fn write_encoded_segment(
    dir: &Path,
    kind: &str,
    generation: u64,
    bytes: &[u8],
) -> io::Result<u64> {
    let file = segment_file_name(kind, generation);
    write_atomic(&dir.join(file), bytes)?;
    Ok(bytes.len() as u64)
}

pub(super) fn write_segment<T: Serialize>(
    dir: &Path,
    magic: [u8; 4],
    kind: &str,
    generation: u64,
    value: &T,
) -> io::Result<u64> {
    let bytes = encode(magic, value)?;
    write_encoded_segment(dir, kind, generation, &bytes)
}

pub(super) fn publish_manifest(backend: &FileSnapshot, manifest: &Manifest) -> io::Result<()> {
    validate_manifest(manifest)?;
    let bytes = encode(MANIFEST_MAGIC, manifest)?;
    write_atomic(&manifest_path(backend), &bytes)
}

pub(super) fn cleanup_after_full_materialization(
    backend: &FileSnapshot,
    keep_base_generation: u64,
) {
    let dir = store_dir(backend);
    let keep_base = segment_file_name("base", keep_base_generation);

    match fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if name == MANIFEST_FILE || name == keep_base {
                    continue;
                }
                if (name.starts_with("base-") || name.starts_with("delta-"))
                    && fs::remove_file(&path)
                        .is_err_and(|err| err.kind() != io::ErrorKind::NotFound)
                {
                    observability::record_snapshot_cleanup_failure();
                }
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(_) => observability::record_snapshot_cleanup_failure(),
    }

    if fs::remove_file(&backend.path)
        .is_err_and(|err| err.kind() != io::ErrorKind::NotFound)
    {
        observability::record_snapshot_cleanup_failure();
    }

    #[cfg(unix)]
    if let Ok(dir_handle) = fs::File::open(&dir) {
        let _ = dir_handle.sync_all();
    }
}

#[cfg(test)]
pub(super) fn paths(backend: &FileSnapshot) -> (PathBuf, PathBuf) {
    (store_dir(backend), manifest_path(backend))
}
