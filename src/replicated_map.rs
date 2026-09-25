// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Provides the [`ReplicatedMap`], a wrapper to a key-value map
//! to enable reconciliation between different instances over a network.

use std::hash::Hash;
use std::io;
use std::net::IpAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::bounds::{Key, Value};
use crate::clock::NodeId;
use crate::discovery::Discovery;
use crate::persistence::{InMemoryPersistence, Persistence};
use crate::replica::Replica;
use crate::timeout_wheel::TimeoutWheel;
use crate::transport::Transport;

mod backpressure;
mod config;
mod construction_error;
mod discovery;
mod membership;
mod mutate;
mod persistence;
mod read;
mod value_size;
mod write;

pub use backpressure::{Backpressure, WriteRejected};
pub(crate) use config::MIN_BULK_SEND_RATE;
pub use config::{Config, ConfigError, MAX_NETS};
pub use construction_error::ConstructionError;
#[cfg(test)]
pub(crate) use discovery::MemberPresence;
pub use discovery::NotAuthoritative;
pub use membership::{RunOutcome, SyncState};
pub use persistence::PersistenceLoadError;
pub use value_size::ValueTooLarge;

/// Default cadence of the dynamic-discovery task (see [`ReplicatedMap::with_discovery_interval`]).
const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// Default number of consecutive discovery rounds a member may be absent before it is
/// decommissioned (see [`ReplicatedMap::with_discovery_miss_threshold`]).
const DEFAULT_DISCOVERY_MISS_THRESHOLD: u32 = 3;

/// Default wall-time floor a member with pending unacknowledged tombstones must be absent for
/// before decommissioning (see [`ReplicatedMap::with_discovery_decommission_floor`]). Ten minutes
/// is far above any DNS blip, so it engages only against a sustainedly wrong resolver.
const DEFAULT_DISCOVERY_DECOMMISSION_FLOOR: Duration = Duration::from_secs(600);

/// Core service wrapping a key-value map, reconciled with peers over the network.
/// Wraps its [`FingerprintTreeMap`](crate::FingerprintTreeMap)'s insertion and deletion; `run`
/// must be called to synchronize. Peers come from [`with_seed`](ReplicatedMap::with_seed) and from
/// periodic probing of the declared networks.
pub struct ReplicatedMap<K, V>
where
    K: Clone + Hash + std::cmp::Eq + Send + Sync,
{
    /// Internal map and hooks container.
    engine: Replica<K, V>,
    /// Tombstone timestamps for deleted entries.
    tombstones: TimeoutWheel<K>,
    /// Durable backend. Always present (the trait is mandatory); defaults to the non-durable
    /// [`InMemoryPersistence`], swapped out via [`with_persistence`](ReplicatedMap::with_persistence).
    persistence: Arc<dyn Persistence<K, V>>,
    /// Optional dynamic peer-discovery source (e.g. Kubernetes DNS). When `None` (the default),
    /// discovery falls back entirely to the per-network random probing in the engine; when set, a
    /// background task injects the discovered peers and decommissions vanished ones.
    discovery: Option<Arc<dyn Discovery>>,
    /// How often the discovery task resolves the peer set.
    discovery_interval: Duration,
    /// Consecutive missed discovery rounds before a vanished member with no pending unacknowledged
    /// tombstones is decommissioned (the fast path).
    discovery_miss_threshold: u32,
    /// Minimum continuous wall-time absence a member with pending unacknowledged tombstones must
    /// additionally clear before it is decommissioned (see
    /// [`with_discovery_decommission_floor`](Self::with_discovery_decommission_floor)).
    discovery_decommission_floor: Duration,
    /// How often [`snapshot_periodically`](Self::snapshot_periodically) wakes to consider writing
    /// a full snapshot, or `None` to disable the periodic task entirely. See
    /// [`Config::snapshot_interval`].
    snapshot_interval: Option<Duration>,
    /// Minimum changes since the last snapshot before a periodic wakeup actually writes one. See
    /// [`Config::snapshot_change_threshold`].
    snapshot_change_threshold: usize,
    /// When the last snapshot (periodic or [`snapshot_now`](Self::snapshot_now)) completed
    /// successfully. Shared across clones — the background snapshot task runs on a clone of the
    /// handle a caller queries [`sync_state`](Self::sync_state) through.
    last_snapshot_at: Arc<RwLock<Option<Instant>>>,
    /// Serialize explicit, periodic and shutdown snapshots across cloned handles.
    snapshot_lock: Arc<Mutex<()>>,
    /// Consecutive snapshot-write failures since the last success; `0` while healthy. Backs the
    /// `reconcile_persistence_failures_current` gauge (behind the `metrics` feature), but tracked
    /// unconditionally since [`on_persistence_error`] callers want it too.
    /// [`on_persistence_error`]: Self::on_persistence_error
    persistence_consecutive_failures: Arc<AtomicUsize>,
    /// Invoked with the [`io::Error`] whenever a snapshot write fails — see
    /// [`on_persistence_error`](Self::on_persistence_error). Defaults to a no-op.
    persistence_error_hook: Arc<dyn Fn(&io::Error) + Send + Sync>,
    /// When [`discover_periodically`](Self::discover_periodically) last resolved the discovery
    /// source successfully, or `None` if it never has (including when no source is configured).
    last_successful_discovery_at: Arc<RwLock<Option<Instant>>>,
}

impl<K, V> Clone for ReplicatedMap<K, V>
where
    K: Clone + Hash + std::cmp::Eq + Send + Sync,
{
    /// Allows cloning of the `ReplicatedMap` handle for lightweight sharing in hooks or tests.
    fn clone(&self) -> Self {
        ReplicatedMap {
            engine: self.engine.clone(),
            tombstones: self.tombstones.clone(),
            persistence: self.persistence.clone(),
            discovery: self.discovery.clone(),
            discovery_interval: self.discovery_interval,
            discovery_miss_threshold: self.discovery_miss_threshold,
            discovery_decommission_floor: self.discovery_decommission_floor,
            snapshot_interval: self.snapshot_interval,
            snapshot_change_threshold: self.snapshot_change_threshold,
            last_snapshot_at: self.last_snapshot_at.clone(),
            snapshot_lock: self.snapshot_lock.clone(),
            persistence_consecutive_failures: self.persistence_consecutive_failures.clone(),
            persistence_error_hook: self.persistence_error_hook.clone(),
            last_successful_discovery_at: self.last_successful_discovery_at.clone(),
        }
    }
}

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// Create a `ReplicatedMap`, binding the gossip UDP socket.
    /// # Errors
    /// If the socket cannot be bound to `(config.listen_addr, config.port)`.
    pub async fn new(config: Config) -> Result<Self, ConstructionError> {
        let snapshot_interval = config.snapshot_interval;
        let snapshot_change_threshold = config.snapshot_change_threshold;
        Ok(Self::from_engine(
            Replica::<K, V>::new(config).await?,
            snapshot_interval,
            snapshot_change_threshold,
        ))
    }

    /// Create a `ReplicatedMap` over a caller-supplied [`Transport`] instead of the default UDP
    /// one — a different datagram transport, or a lossy one to test convergence under adversity.
    /// The caller has already done the I/O binding step; configuration validation remains
    /// fallible. An unreliable transport cannot violate an invariant, since the protocol already
    /// assumes loss, duplication and reordering — unlike an injected [`Clock`](crate::Clock)
    /// ([`new_with_clock`](Self::new_with_clock)'s docs cover what a non-conformant one breaks).
    pub fn new_with_transport(
        config: Config,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, ConstructionError> {
        let snapshot_interval = config.snapshot_interval;
        let snapshot_change_threshold = config.snapshot_change_threshold;
        Ok(Self::from_engine(
            Replica::<K, V>::with_transport(config, transport)?,
            snapshot_interval,
            snapshot_change_threshold,
        ))
    }

    /// Create a `ReplicatedMap` over UDP with a caller-supplied [`Clock`](crate::Clock).
    ///
    /// The clock defines timestamp ordering for all local and observed writes. It must satisfy the
    /// [`Clock`](crate::Clock) contract; [`assert_conformance`](crate::clock::assert_conformance)
    /// can validate an implementation before use.
    pub async fn new_with_clock(
        config: Config,
        clock: Arc<dyn crate::clock::Clock>,
    ) -> Result<Self, ConstructionError> {
        let snapshot_interval = config.snapshot_interval;
        let snapshot_change_threshold = config.snapshot_change_threshold;
        Ok(Self::from_engine(
            Replica::<K, V>::new_with_clock(config, clock).await?,
            snapshot_interval,
            snapshot_change_threshold,
        ))
    }

    /// Wrap a constructed engine in the store's own bookkeeping (tombstone wheel, persistence,
    /// discovery defaults). The single place those defaults are spelled out, so the constructors
    /// above cannot drift apart.
    fn from_engine(
        engine: Replica<K, V>,
        snapshot_interval: Option<Duration>,
        snapshot_change_threshold: usize,
    ) -> Self {
        let svc = ReplicatedMap {
            engine,
            tombstones: TimeoutWheel::new(),
            persistence: Arc::new(InMemoryPersistence::default()),
            discovery: None,
            discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
            discovery_miss_threshold: DEFAULT_DISCOVERY_MISS_THRESHOLD,
            discovery_decommission_floor: DEFAULT_DISCOVERY_DECOMMISSION_FLOOR,
            snapshot_interval,
            snapshot_change_threshold,
            last_snapshot_at: Arc::new(RwLock::new(None)),
            snapshot_lock: Arc::new(Mutex::new(())),
            persistence_consecutive_failures: Arc::new(AtomicUsize::new(0)),
            persistence_error_hook: Arc::new(|_: &io::Error| {}),
            last_successful_discovery_at: Arc::new(RwLock::new(None)),
        };
        svc.set_pre_insert(|_, _| {});
        svc
    }

    /// This node's HLC identity: the `node_id` on every [`Timestamp`](crate::clock::Timestamp) it mints.
    /// Random per construction unless pinned with [`Config::with_node_id`].
    pub fn node_id(&self) -> NodeId {
        self.engine.node_id()
    }

    /// Provides the address of a known peer to the store
    /// This is optional, but reduces the time to connect to existing peers
    pub fn with_seed(self, peer: IpAddr) -> Self {
        let now = Instant::now();
        self.engine.peers.write().insert(peer, now);
        self
    }

    /// Register or refresh a known peer at runtime — the `&self` counterpart of
    /// [`with_seed`](Self::with_seed), and what a discovery source feeds in.
    /// Re-arms the peer-expiration window and makes the address a gossip target. Never grants
    /// causal-stability membership.
    pub fn seed_peer(&self, peer: IpAddr) {
        self.engine.seed_peer(peer);
    }
}

#[cfg(test)]
mod tests;
