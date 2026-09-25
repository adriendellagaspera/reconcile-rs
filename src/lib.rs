// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Embedded, in-memory, eventually consistent replication.
//!
//! [`ReplicatedMap`] stores the complete dataset on every authoritative replica. Reads are local;
//! writes propagate asynchronously and same-key conflicts use last-write-wins ordering.
//!
//! Use it when the working set fits in memory on every node and eventual consistency is acceptable.
//! It is not a counter CRDT, a strongly consistent store, or a sharded data store.
//!
//! Construction requires an explicit trust mode. Use
//! [`Config::with_cluster_key`](replicated_map::Config::with_cluster_key) for authenticated traffic,
//! or [`Config::with_insecure_no_key`](replicated_map::Config::with_insecure_no_key) only when the
//! surrounding network supplies the trust boundary.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod clock;
/// Public, stable metric names — see the module's own docs.
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod persistence;
pub mod read_replica_map;
pub mod read_replica_set;
pub mod replicated_map;
pub mod replicated_set;
pub(crate) mod snapshot;
pub mod value_ref;

// Stable facade re-exports.
pub use gossip::auth::{ClusterKey, ClusterKeyError};
pub use gossip::{discovery, transport};
pub use lww_register::{bounds, entry};

// Re-exported so no public signature that names one of these types — `Config::nets`'
// `ipnet::IpNet`, `UdpTransport::new`/`socket`'s `tokio::net::UdpSocket`, `RandomProbe::new`'s
// `parking_lot`/`rand` parameters, `Transport`'s `#[async_trait]` — forces a dependent onto an
// independently-versioned copy of that crate. `bincode` and `metrics-exporter-prometheus` are
// deliberately not re-exported this way: their errors are wrapped instead (`gossip::bincode`,
// `prometheus.rs`) because they are an implementation choice, not part of the contract.
pub use gossip::async_trait;
pub use ipnet;
pub use parking_lot;
pub use rand;
pub use tokio;
pub use tokio_util;

/// Optional Prometheus integration (enabled by the `metrics-prometheus` feature).
#[cfg(feature = "metrics-prometheus")]
pub mod prometheus;

// Internal runtime mechanisms; test seams go through [`testing`].
pub(crate) mod observability;
pub(crate) mod replica;
pub(crate) mod timeout_wheel;

pub use bounds::{Key, Value};
pub use clock::{
    Clock, ClockDrift, Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp, MAX_CLOCK_DRIFT,
};
pub use discovery::{
    DiscoverFuture, Discovery, DiscoveryError, DiscoveryKind, DnsDiscovery, DnsDiscoveryError,
    RandomProbe,
};
pub use entry::{Entry, State};
pub use transport::{InMemoryNetwork, InMemoryTransport, Transport, UdpTransport};
pub use value_ref::ValueRef;
// `IterMut`/`ValuesMut` are deliberately not re-exported: they leave fingerprints stale.
// `FingerprintTreeMap::with_mut` is the supported mutation path.
pub use rsos::{
    Aggregate, Fingerprint, FingerprintTreeMap, IntoIter, IntoKeys, IntoValues, ItemRange, Iter,
    Keys, Rsos, Values,
};

pub use persistence::{FileSnapshot, InMemoryPersistence, PersistedState, Persistence};
pub use read_replica_map::ReadReplicaMap;
pub use read_replica_set::ReadReplicaSet;
pub use replicated_map::ReplicatedMap;
pub use replicated_set::ReplicatedSet;

/// Internal seam for the integration tests, behind `cfg(test)` or `cfg(reconcile_internal_testing)`.
///
/// Carries only what stays crate-internal: `rbsr`/`rsos` primitives are `pub` on their own crates
/// and are imported directly.
#[doc(hidden)]
#[cfg(any(test, reconcile_internal_testing))]
pub mod testing {
    /// Seal `payload` with MAC authentication: `tag(32) || seq(8 LE) || stamp(8 LE) ||
    /// version(1) || payload` (the wire-version byte).
    pub fn seal_datagram(key: [u8; 32], seq: u64, stamp: u64, payload: &[u8]) -> Vec<u8> {
        gossip::auth::Authenticator::new(Some(gossip::auth::ClusterKey::new(key)), false)
            .unwrap()
            .seal(
                gossip::replay::Seq::new(seq),
                gossip::replay::Stamp::new(stamp),
                payload,
            )
    }

    /// The current causal-stability membership set.
    pub fn members_snapshot<K, V>(
        store: &crate::ReplicatedMap<K, V>,
    ) -> std::collections::HashSet<std::net::IpAddr>
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.members_snapshot()
    }

    /// Number of entries in the peers gossip-routing map.
    pub fn peers_map_len<K, V>(store: &crate::ReplicatedMap<K, V>) -> usize
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.peers_map_len()
    }

    /// Number of entries in the per-peer replay filter.
    pub fn replay_filter_len<K, V>(store: &crate::ReplicatedMap<K, V>) -> usize
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.replay_filter_len()
    }

    /// Number of keys tracked in the tombstone-acknowledgment map.
    pub fn tombstone_acks_len<K, V>(store: &crate::ReplicatedMap<K, V>) -> usize
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.tombstone_acks_len()
    }

    /// Number of bulk dump tasks in flight across all peers.
    pub fn bulk_dumps_in_flight_count<K, V>(store: &crate::ReplicatedMap<K, V>) -> usize
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.bulk_dumps_in_flight_count()
    }

    /// Number of write-broadcast tasks currently in flight.
    pub fn broadcasts_in_flight_count<K, V>(store: &crate::ReplicatedMap<K, V>) -> usize
    where
        K: crate::bounds::Key + std::hash::Hash,
        V: crate::bounds::Value,
    {
        store.broadcasts_in_flight_count()
    }
}
