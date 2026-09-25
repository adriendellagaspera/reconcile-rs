// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//!  manual structural probe, after 's boxed children array.
//! These are exact Rust layout/occupancy counts, not measured RSS or a predicted heap saving.
//! Pair with the allocation-counting system::heap_footprint benchmark on the same revision.

use std::mem::size_of;

use super::super::{node::Children, FingerprintTreeMap, Node, MAX_CAPACITY};
use crate::fingerprint::Fingerprint;

#[derive(Default)]
struct Occupancy {
    nodes: usize,
    leaves: usize,
    elements: usize,
    max_depth: usize,
}

fn visit(node: &Node<u32, u32>, depth: usize, counts: &mut Occupancy) {
    counts.nodes += 1;
    counts.elements += node.keys.len();
    counts.max_depth = counts.max_depth.max(depth);
    match node.children.as_ref() {
        None => counts.leaves += 1,
        Some(children) => {
            for child in children.iter() {
                visit(child, depth + 1, counts);
            }
        }
    }
}

fn report(n: usize, kind: &str, tree: &FingerprintTreeMap<u32, u32>) {
    let mut counts = Occupancy::default();
    visit(&tree.root, 1, &mut counts);
    assert_eq!(counts.elements, n);
    let allocated_fingerprint_slots = counts.nodes * MAX_CAPACITY;
    let child_array_bytes = (counts.nodes - counts.leaves) * size_of::<Children<u32, u32>>();
    println!(
        "{kind},{n},{},{},{},{},{:.4},{},{},{}",
        counts.nodes,
        counts.leaves,
        counts.max_depth,
        counts.elements,
        counts.elements as f64 / allocated_fingerprint_slots as f64,
        allocated_fingerprint_slots * size_of::<Fingerprint>(),
        allocated_fingerprint_slots * size_of::<Fingerprint>() / n,
        child_array_bytes
    );
}

/// Manual probe: cargo test -p rsos --lib node_occupancy -- --ignored --nocapture
/// RECONCILE_BASELINE_SIZES=10000,100000,1000000 extends the default sweep.
/// A per-node inline fingerprint reservation is not a guaranteed equal-sized saving when the
/// field is removed: alignment, allocation classes and other bookkeeping affect actual RSS.
#[test]
#[ignore = "manual #47 memory-layout baseline; do not run a large sweep in CI"]
fn node_occupancy() {
    println!(
        "node_u32_bytes={},node_heap_types_bytes={},fingerprint_bytes={},boxed_children_array_bytes={}",
        size_of::<Node<u32, u32>>(),
        size_of::<Node<String, Vec<u8>>>(),
        size_of::<Fingerprint>(),
        size_of::<Children<u32, u32>>()
    );
    println!(
        "build,n,nodes,leaves,depth,elements,occupancy,reserved_fingerprint_bytes,reserved_fingerprint_bytes_per_entry,boxed_children_bytes"
    );
    let sweep =
        std::env::var("RECONCILE_BASELINE_SIZES").unwrap_or_else(|_| "10000,100000".to_owned());
    for part in sweep.split(',') {
        let n = part.trim().parse::<usize>().expect("valid n");
        assert!(n > 0 && n <= u32::MAX as usize);
        let serial: FingerprintTreeMap<u32, u32> = (0..n as u32).map(|k| (k, k)).collect();
        report(n, "serial", &serial);
        let bulk = FingerprintTreeMap::from_sorted_iter((0..n as u32).map(|k| (k, k)));
        report(n, "bulk", &bulk);
        assert_eq!(serial.aggregate(..), bulk.aggregate(..));
    }
}
