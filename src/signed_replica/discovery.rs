// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use gossip::discovery::EndpointDiscoveryState;

use super::*;

impl<K: Key + Hash, V: Value, A: Clone + Eq> SignedReplica<K, V, A> {
    /// Produce a bounded, rate-limited batch of signed hello probes from discovery hints.
    ///
    /// Refresh discovery separately, so resolver latency cannot hold this actor's borrow.
    /// Untrusted hints never confirm a route, change the provisioned authoritative roster or
    /// satisfy tombstone ACKs. Empty discovery and missing/failed hints do not decommission
    /// peers. Deliver these datagrams and drive `receive` for signed route confirmation.
    ///
    /// # Errors
    /// A signed hello cannot be encoded or sealed. Taking candidates consumes the batch's
    /// cooldown even if encoding or later substrate delivery fails.
    pub fn probe_discovered(
        &self,
        discovery: &mut EndpointDiscoveryState<A>,
        now: Instant,
    ) -> io::Result<Vec<RoutedDatagram<A>>> {
        discovery
            .next_probes(now)
            .into_iter()
            .map(|endpoint| self.probe(endpoint))
            .collect()
    }
}
