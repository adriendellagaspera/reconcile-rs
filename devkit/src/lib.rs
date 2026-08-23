// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Bench/measurement infrastructure with no natural home among the five domain crates: this
//! workspace's own `benches/` and the private research companion repository both need it, without
//! duplicating it. Never published (`AGENTS.md` §1, #524) — a dev/bench-only sibling, exempt from
//! `check-domain-purity.sh` the way `gossip`/`reconcile` already are.
//!
//! Three independent pieces, one per module:
//!
//! - [`stats`]: nonparametric bootstrap summary statistics for repeated trials.
//! - [`protocol_cost`]: the `Cost`/`Counting` reconciliation-cost driver, generic over any
//!   `rsos::Rsos` backend and any `rbsr::RefinementPolicy`.
//! - [`contention`]: the paired-trial N-writer harness, generic over any two named
//!   [`contention::ContentionTarget`] implementations.

#![forbid(unsafe_code)]

pub mod contention;
pub mod protocol_cost;
pub mod stats;
