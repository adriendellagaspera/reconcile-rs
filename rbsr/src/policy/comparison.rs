// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`Comparison`] construction and the read-only accessors [`RefinementPolicy`] is soundly allowed
//! to see.
//! [`RefinementPolicy`]: super::RefinementPolicy

use super::Comparison;
use rsos::Aggregate;

impl Comparison {
    /// Build a comparison. Public so a policy can be unit-tested without a driver.
    pub const fn new(local: Aggregate, remote: Aggregate, children_emitted: usize) -> Comparison {
        Comparison {
            local,
            remote,
            children_emitted,
        }
    }

    /// `|X ∩ [l, u)|`: **local** elements covered — what `t` is compared against, and what a
    /// [`Decision::Split`](super::Decision::Split) cuts, since a split is by local rank.
    pub const fn span(&self) -> usize {
        self.local.size()
    }

    /// `|Y ∩ [l, u)|`: **remote** elements covered, as advertised. Unauthenticated peer input —
    /// readable, never to be assumed true.
    pub const fn remote_size(&self) -> usize {
        self.remote.size()
    }

    /// Whether the range is already resolved.
    /// Compares cardinality and fingerprint together.
    pub fn agrees(&self) -> bool {
        self.local == self.remote
    }

    /// Child ranges already emitted this round: the round-budget seam.
    /// Counted in ranges, not bytes — this crate owns no encoding. No shipped policy reads it;
    /// [`RefinementPolicy`](super::RefinementPolicy) carries a worked capping example.
    pub const fn children_emitted(&self) -> usize {
        self.children_emitted
    }

    /// Test-only access to the full local [`Aggregate`] for probe/oracle tests.
    /// Shipped policies intentionally use [`span`](Self::span) and [`remote_size`](Self::remote_size).
    #[cfg(reconcile_internal_testing)]
    pub const fn local_for_testing(&self) -> Aggregate {
        self.local
    }

    /// **Test-only, `cfg(reconcile_internal_testing)`-gated.** The whole **remote** [`Aggregate`],
    /// fingerprint included — the peer-advertised counterpart to
    /// [`local_for_testing`](Self::local_for_testing), same caveats.
    #[cfg(reconcile_internal_testing)]
    pub const fn remote_for_testing(&self) -> Aggregate {
        self.remote
    }
}
