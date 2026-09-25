// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Transport-independent Range-Based Set Reconciliation.
//!
//! [`initial_ranges`] starts a reconciliation. [`protocol_round`] advances one side by comparing
//! [`RangeAggregate`] values and producing ranges to enumerate or refine. Callers alternate rounds
//! until no active ranges remain.
//!
//! [`RsosView`] is the read-only store contract and is implemented for every [`rsos::Rsos`].
//! [`RefinementPolicy`] controls local refinement only; it is never negotiated on the wire.
//!
//! The implementation follows Meyer, *Range-Based Set Reconciliation* (arXiv:2212.13567) over the
//! RSOS interface described by Amparore (arXiv:2603.19820). Equality uses both range cardinality and
//! fingerprint.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod policy;
#[cfg(reconcile_internal_testing)]
mod probe_harness;
mod protocol;
mod rsos_view;

pub use policy::{
    Comparison, Decision, EnumerateBelowThreshold, FanOut, FixedFanOut, RefinementPolicy,
    SplitStride, SqrtFanOut,
};
#[cfg(reconcile_internal_testing)]
pub use policy::{ConstantStrideSplit, SpanHashedStrideSplit, STRIDE_SPREAD};
// Repository-only probe harness.
#[cfg(reconcile_internal_testing)]
pub use probe_harness::{
    balanced_swap, drive, drive_pair, Drive, NarrowStore, Termination, DRIVE_STORE_SIZE,
};
pub use protocol::{
    initial_ranges, protocol_round, protocol_round_with_policy, EnumerationRange, RangeAggregate,
    RoundOutcome,
};
pub use rsos_view::RsosView;

// Keep the `rsos` version used by public signatures directly reachable.
pub use rsos;
