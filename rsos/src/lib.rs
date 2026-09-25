// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Range-Summarizable Order-Statistics Store.
//!
//! [`Rsos`] defines ordered storage with range aggregate, rank, select, enumerate, insert, and
//! delete operations. [`FingerprintTreeMap`] is the in-memory B-tree implementation.
//!
//! [`Aggregate`] combines cardinality with a 256-bit additive [`Fingerprint`]. Fingerprints are
//! computed from this crate's canonical [`encoding`], so elements require
//! [`Serialize`](serde::Serialize) rather than [`Hash`](std::hash::Hash).
//!
//! The API follows Amparore, *Range-Based Set Reconciliation via Range-Summarizable
//! Order-Statistics Stores* (arXiv:2603.19820).
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod aggregate;
// Repository-only measurement seam.
#[cfg(reconcile_internal_testing)]
pub mod counters;
#[cfg(not(reconcile_internal_testing))]
mod counters;
pub mod encoding;
pub mod fingerprint;
pub mod fingerprint_tree_map;
pub mod fingerprint_tree_map_iter;
mod rsos_trait;

pub use aggregate::Aggregate;
pub use fingerprint::{digest, digest_keyed, lift, lift_keyed, Fingerprint, LiftKey};
pub use fingerprint_tree_map::{Entry, FingerprintTreeMap, ItemRange, OwnedValueRef};
pub use fingerprint_tree_map_iter::{IntoIter, IntoKeys, IntoValues, Iter, Keys, Values};
pub use rsos_trait::Rsos;

// Re-exported so a third party building a `lift`-compatible `Fingerprint` from raw bytes (via
// `Fingerprint::from_le_bytes` and `encoding::Sink for blake3::Hasher`) never needs its own,
// independently-versioned `blake3` dependency.
pub use blake3;
