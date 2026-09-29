use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::storage::{
    decode, manifest_path, publish_manifest, read_manifest, store_dir, validate_manifest,
    write_atomic, write_segment, OBJECT_CHECKSUM_LEN,
};
use super::*;

fn policy_manifest(delta_count: u64, delta_bytes: u64) -> Manifest {
    Manifest {
        base_generation: 10,
        base_bytes: 1_000,
        current_generation: 10 + delta_count,
        delta_bytes,
    }
}

#[test]
fn materialization_byte_trigger_is_exactly_one_base() {
    let manifest = policy_manifest(1, 400);
    assert!(!should_materialize(&manifest, 599));
    assert!(should_materialize(&manifest, 600));
}

#[test]
fn materialization_count_cap_is_exactly_512_deltas() {
    let below = policy_manifest(MAX_COMMITTED_DELTAS - 1, 1);
    assert!(!should_materialize(&below, 1));

    let at_cap = policy_manifest(MAX_COMMITTED_DELTAS, 1);
    assert!(should_materialize(&at_cap, 1));
}

#[test]
fn manifest_reports_committed_segments_and_bytes() {
    let manifest = policy_manifest(37, 250);
    assert_eq!(manifest.delta_count(), 37);
    assert_eq!(manifest.segment_count(), 38);
    assert_eq!(manifest.segment_bytes(), 1_250);
}

#[test]
fn manifest_rejects_invalid_generation_bounds() {
    let manifest = Manifest {
        base_generation: 2,
        base_bytes: 1,
        current_generation: 1,
        delta_bytes: 0,
    };
    let err = validate_manifest(&manifest).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let manifest = Manifest {
        base_generation: 0,
        base_bytes: 1,
        current_generation: 1,
        delta_bytes: 0,
    };
    assert!(validate_manifest(&manifest).is_err());
}

#[test]
fn manifest_rejects_inconsistent_delta_byte_accounting() {
    let no_deltas = Manifest {
        base_generation: 1,
        base_bytes: 1,
        current_generation: 1,
        delta_bytes: 7,
    };
    assert!(validate_manifest(&no_deltas).is_err());

    let committed_delta = Manifest {
        base_generation: 1,
        base_bytes: 1,
        current_generation: 2,
        delta_bytes: 0,
    };
    assert!(validate_manifest(&committed_delta).is_err());
}

#[test]
fn manifest_rejects_zero_base_size() {
    let manifest = Manifest {
        base_generation: 1,
        base_bytes: 0,
        current_generation: 1,
        delta_bytes: 0,
    };
    assert!(validate_manifest(&manifest).is_err());
}

#[test]
fn recovery_rejects_incorrect_committed_byte_totals() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let state = PersistedState::<i32, String>::default();
    save_full(&backend, &state).unwrap();

    let mut manifest = read_manifest(&backend).unwrap().unwrap();
    manifest.base_bytes = manifest.base_bytes.saturating_add(1);
    publish_manifest(&backend, &manifest).unwrap();
    let err = load::<i32, String>(&backend).unwrap_err();
    assert!(err.to_string().contains("manifest records"));

    save_full(&backend, &state).unwrap();
    let mut manifest = read_manifest(&backend).unwrap().unwrap();
    let delta = PersistenceDelta::<i32, String>::new(
        HashMap::new(),
        HashMap::new(),
        HashSet::new(),
        HashMap::new(),
    );
    let generation = manifest.current_generation.saturating_add(1);
    let segment = DeltaSegmentWrite {
        from_generation: manifest.current_generation,
        to_generation: generation,
        delta: &delta,
    };
    let bytes = write_segment(
        &store_dir(&backend),
        DELTA_MAGIC,
        "delta",
        generation,
        &segment,
    )
    .unwrap();
    manifest.current_generation = generation;
    manifest.delta_bytes = bytes.saturating_add(1);
    publish_manifest(&backend, &manifest).unwrap();
    let err = load::<i32, String>(&backend).unwrap_err();
    assert!(err.to_string().contains("committed delta bytes"));
}

#[test]
fn framing_rejects_short_objects_but_reaches_decode_at_exact_minimum() {
    let short = vec![0u8; HEADER_LEN + OBJECT_CHECKSUM_LEN - 1];
    let err = decode::<u8>(&short, MANIFEST_MAGIC).unwrap_err();
    assert!(err.to_string().contains("shorter"));

    let mut exact = Vec::new();
    exact.extend_from_slice(&MANIFEST_MAGIC);
    exact.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    exact.extend_from_slice(blake3::hash(&[]).as_bytes());
    assert_eq!(exact.len(), HEADER_LEN + OBJECT_CHECKSUM_LEN);
    let err = decode::<u8>(&exact, MANIFEST_MAGIC).unwrap_err();
    assert!(
        !err.to_string().contains("shorter"),
        "exact header+checksum length must reach body decoding"
    );
}

#[test]
fn read_manifest_propagates_non_not_found_errors() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let manifest = manifest_path(&backend);
    std::fs::create_dir_all(&manifest).unwrap();

    assert!(
        read_manifest(&backend).is_err(),
        "a non-file manifest path must not be treated as an absent manifest"
    );
}

#[test]
fn atomic_write_supports_relative_paths() {
    let name = format!("reconcile-snapshot-relative-{}", std::process::id());
    let path = Path::new(&name);
    let _ = std::fs::remove_file(path);

    write_atomic(path, b"payload").unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"payload");
    std::fs::remove_file(path).unwrap();
}

fn assert_bad_delta_generation(from_generation: u64, to_generation: u64) {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileSnapshot::new(dir.path().join("snapshot.bin"));
    let state = PersistedState::<i32, String>::default();
    save_full(&backend, &state).unwrap();

    let mut manifest = read_manifest(&backend).unwrap().unwrap();
    assert_eq!(manifest.current_generation, 1);
    let delta = PersistenceDelta::<i32, String>::new(
        HashMap::new(),
        HashMap::new(),
        HashSet::new(),
        HashMap::new(),
    );
    let segment = DeltaSegmentWrite {
        from_generation,
        to_generation,
        delta: &delta,
    };
    let bytes = write_segment(&store_dir(&backend), DELTA_MAGIC, "delta", 2, &segment).unwrap();
    manifest.current_generation = 2;
    manifest.delta_bytes = bytes;
    publish_manifest(&backend, &manifest).unwrap();

    let err = load::<i32, String>(&backend).unwrap_err();
    assert!(
        err.to_string().contains("delta continuity"),
        "malformed delta generation must be rejected: {err}"
    );
}

#[test]
fn delta_from_generation_must_match_manifest_slot() {
    assert_bad_delta_generation(0, 2);
}

#[test]
fn delta_to_generation_must_match_manifest_slot() {
    assert_bad_delta_generation(1, 99);
}
