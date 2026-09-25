// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

/// An isolated datagram transport for tests that exercise engine logic, not UDP binding.
/// Every call gets a private routing table, so the virtual port cannot collide with another test.
pub(crate) fn in_memory_test_replica<K, V>(
    config: crate::replicated_map::Config,
) -> crate::replica::Replica<K, V>
where
    K: crate::bounds::Key + std::hash::Hash,
    V: crate::bounds::Value,
{
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::transport::InMemoryNetwork;

    let endpoint = SocketAddr::new(config.listen_addr, config.port);
    let network = InMemoryNetwork::new();
    crate::replica::Replica::with_transport(config, Arc::new(network.bind(endpoint)))
        .expect("valid test config")
}

mod auth_attack;
mod broadcast_budget;
mod causal_stability;
mod clock_drift;
mod clock_port;
mod coalescing;
mod convergence_ack;
mod deadlock_regressions;
mod dump_budget;
mod equal_stamp_redelivery;
mod handle_messages_return_value;
mod immediate_broadcast;
mod in_memory_convergence;
mod keyed_fingerprint;
mod pacing;
mod pending_dump_requeue;
mod receiver_bulk_guard;
mod repair;
mod reserved_wire_tags;
mod socket_buffers;
mod tombstone_ack_bounds;
mod tombstone_ack_resend_counting;
