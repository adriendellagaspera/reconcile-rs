use super::storage::validate_manifest;
use super::*;

fn reference(generation: u64) -> SegmentRef {
    SegmentRef {
        generation,
        file: format!("delta-{generation:020}.bin"),
        bytes: 0,
        checksum: [0; 32],
    }
}

#[test]
fn manifest_requires_contiguous_committed_generations() {
    let manifest = Manifest {
        base: SegmentRef {
            generation: 1,
            file: "base-00000000000000000001.bin".to_string(),
            bytes: 0,
            checksum: [0; 32],
        },
        deltas: vec![reference(2), reference(4)],
        current_generation: 4,
    };
    let err = validate_manifest(&manifest).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("continuity"));
}
