use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::storage::{
    decode, manifest_path, publish_manifest, read_manifest, store_dir, validate_manifest,
    validate_segment_name, write_atomic, write_segment, OBJECT_CHECKSUM_LEN,
};
use super::*;

fn reference_with_bytes(generation: u64, bytes: u64) -> SegmentRef {
    SegmentRef {
        generation,
        file: format!("delta-{generation:020}.bin"),
        bytes,
    }
}

fn reference(generation: u64) -> SegmentRef {
    reference_with_bytes(generation, 0)
}

#[test]
fn manifest_requires_contiguous_committed_generations() {
    let manifest = Manifest {
        base: SegmentRef {
            generation: 1,
            file: "base-00000000000000000001.bin".to_string(),
            bytes: 0,
        },
        deltas: vec![reference(2), reference(4)],
        current_generation: 4,
    };
    let err = validate_manifest(&manifest).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("continuity"));
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
fn segment_names_must_be_single_normal_components() {
    assert!(validate_segment_name("base-00000000000000000001.bin").is_ok());
    for bad in ["../base.bin", "sub/base.bin", "/tmp/base.bin", "."] {
        assert!(
            validate_segment_name(bad).is_err(),
            "segment path {bad:?} must be rejected"
        );
    }
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
    let reference = write_segment(&store_dir(&backend), DELTA_MAGIC, "delta", 2, &segment).unwrap();
    manifest.deltas.push(reference);
    manifest.current_generation = 2;
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


fn policy_manifest(base_bytes: u64, delta_bytes: &[u64]) -> Manifest {
    let base = SegmentRef {
        generation: 1,
        file: "base-00000000000000000001.bin".to_string(),
        bytes: base_bytes,
    };
    let deltas: Vec<_> = delta_bytes
        .iter()
        .enumerate()
        .map(|(index, bytes)| reference_with_bytes(index as u64 + 2, *bytes))
        .collect();
    Manifest {
        current_generation: deltas
            .last()
            .map_or(base.generation, |delta| delta.generation),
        base,
        deltas,
    }
}

#[test]
fn adaptive_policy_caps_committed_delta_chain_at_32() {
    let thirty_one = policy_manifest(1_000_000, &vec![100; 31]);
    assert!(!should_materialize(&thirty_one, 100));

    let thirty_two = policy_manifest(1_000_000, &vec![100; 32]);
    assert!(
        should_materialize(&thirty_two, 100),
        "publishing a 33rd delta must fall back to a fresh base"
    );
}

#[test]
fn adaptive_policy_compacts_at_one_base_equivalent_of_delta_bytes() {
    let manifest = policy_manifest(1_000, &[200, 300]);
    assert!(!should_materialize(&manifest, 499));
    assert!(
        should_materialize(&manifest, 500),
        "candidate delta that reaches one base-equivalent must compact"
    );
}
