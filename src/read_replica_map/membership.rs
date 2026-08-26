// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #30: the introspection accessors a read replica can actually support. `sync_state`/`peers`
//! mirror [`ReplicatedMap`](crate::ReplicatedMap)'s #292 pair; `local_addr` mirrors it exactly.
//! `members()`/`node_id()` have no counterpart here and are not added — see their doc notes below
//! for why.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ipnet::IpNet;

use crate::bounds::{Key, Value};

use super::ReadReplicaMap;

const PEER_EXPIRATION: Duration = Duration::from_secs(60);

/// A snapshot of [`ReadReplicaMap`]'s liveness, for a caller building its own readiness signal —
/// the read-replica counterpart of [`SyncState`](crate::replicated_map::SyncState) (#30).
///
/// No `last_snapshot_at` field: a read replica never persists (module docs) — cold-starting empty
/// and re-syncing from the dated cluster is the deliberate design, not a gap needing a matching
/// snapshot-liveness field.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ReadSyncState {
    /// Value-only reconciliation rounds initiated since construction.
    pub rounds: u64,
    /// When the most recently initiated round started, or `None` before the first one.
    pub last_round_at: Option<Instant>,
    /// Current size of the gossip-routing peer set — see [`ReadReplicaMap::peers`].
    pub peers: usize,
}

impl<K: Key, V: Value> ReadReplicaMap<K, V> {
    /// (runtime) Retune the network this read replica probes for discovery, visible to all clones.
    ///
    /// Unlike [`ReplicatedMap`](crate::ReplicatedMap)'s `nets`/`add_net`/`remove_net`/`local_net`
    /// (four more methods, #30), a read replica declares only **one** network at a time: it runs no
    /// cross-network gossip to throttle (`remote_interval`/`remote_fanout` are ignored — see
    /// `warn_on_ignored_config_fields`), so there is no local/remote split for a second declared net
    /// to drive. Multiple regions still work — point each read replica's `net` at its own region and
    /// [`with_seed`](Self::with_seed)/discovery still finds cross-region dated peers — just without
    /// the throttled WAN fan-out a dated store's multi-net topology buys.
    pub fn set_net(&self, net: IpNet) {
        *self.net.write() = net;
    }

    /// The network this read replica currently probes for discovery.
    pub fn net(&self) -> IpNet {
        *self.net.read()
    }

    /// The current gossip-routing peer set: addresses seen recently enough to still be
    /// reconciliation targets (#30, mirroring
    /// [`ReplicatedMap::peers`](crate::ReplicatedMap::peers)).
    ///
    /// No `members()` counterpart: a read replica holds no causal-stability membership (module
    /// docs) — every peer here is equally provisional, so there is no stronger "recorded, GC-gating"
    /// tier to distinguish from this weaker one.
    pub fn peers(&self) -> Vec<IpAddr> {
        let mut guard = self.peers.write();
        guard.retain(|_, instant| instant.elapsed() < PEER_EXPIRATION);
        guard.keys().cloned().collect()
    }

    /// The transport's actual bound local address — mirrors
    /// [`ReplicatedMap::local_addr`](crate::ReplicatedMap::local_addr) (#30).
    ///
    /// No `node_id()` counterpart: a read replica mints no [`Timestamp`](crate::clock::Timestamp)s
    /// (module docs, `Config::node_id` is ignored — see `warn_on_ignored_config_fields`), so it has
    /// no HLC identity to report.
    ///
    /// # Errors
    ///
    /// If the underlying transport fails to report its local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    /// (runtime) Retune [`run`](Self::run)'s idle-activity timeout in place — how long it waits for
    /// inbound activity before re-initiating a value-only round. See [`Config::reconcile_interval`](super::Config::reconcile_interval).
    ///
    /// Mirrors [`ReplicatedMap::set_reconcile_interval`](crate::ReplicatedMap::set_reconcile_interval)
    /// (#30: previously a private, unconfigurable one-second constant).
    pub fn set_reconcile_interval(&self, interval: Duration) {
        *self.reconcile_interval.write() = interval;
    }

    /// A snapshot of liveness for a caller building its own readiness signal — see
    /// [`ReadSyncState`] (#30, mirroring
    /// [`ReplicatedMap::sync_state`](crate::ReplicatedMap::sync_state)).
    pub fn sync_state(&self) -> ReadSyncState {
        ReadSyncState {
            rounds: self.round.load(Ordering::Relaxed),
            last_round_at: *self.last_round_at.read(),
            peers: self.peers().len(),
        }
    }
}
