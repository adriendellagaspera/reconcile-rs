// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`ReadReplicaSet`]: a read-only replica of a [`ReplicatedSet`](crate::ReplicatedSet), mirroring
//! [`ReadReplicaMap`] the way [`ReplicatedSet`](crate::ReplicatedSet) mirrors
//! [`ReplicatedMap`](crate::ReplicatedMap).
//!
//! A thin newtype over [`ReadReplicaMap<K, ()>`](crate::ReadReplicaMap), not a reimplementation:
//! same wire format and convergence semantics. It exposes membership (`contains`, `len`,
//! `is_empty`, `keys`) instead of the value-shaped read API (`get`, `values`, `to_vec`,
//! `first_key_value`/`last_key_value`, `for_each*`) — none of it has a meaningful reading when
//! every value is `()`. Reach for `ReadReplicaMap<K, ()>` directly if one of those is genuinely
//! needed.

use std::io;
use std::net::IpAddr;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use ipnet::IpNet;

use crate::bounds::Key;
use crate::discovery::Discovery;
use crate::entry::State;
use crate::read_replica_map::ReadReplicaMap;
use crate::replicated_map::Config;
use rsos::Fingerprint;

/// A read-only replica of a replicated set; see the
/// [module documentation](crate::read_replica_set).
///
/// ```
/// use reconcile::{replicated_map::Config, ReadReplicaSet};
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let set = ReadReplicaSet::<String>::new(Config::new(8083).with_insecure_no_key()).await?;
///
/// // Read-only: nothing arrives until it reconciles with a dated peer (module docs).
/// assert!(!set.contains(&"a".to_string()));
/// # Ok(())
/// # }
/// ```
pub struct ReadReplicaSet<K>(ReadReplicaMap<K, ()>);

impl<K> Clone for ReadReplicaSet<K> {
    /// Allows cloning the `ReadReplicaSet` handle for lightweight sharing in hooks or tests.
    fn clone(&self) -> Self {
        ReadReplicaSet(self.0.clone())
    }
}

impl<K: Key> ReadReplicaSet<K> {
    /// Create a read replica bound to the configured UDP socket. See [`ReadReplicaMap::new`].
    ///
    /// # Errors
    ///
    /// If the socket cannot be bound to `(config.listen_addr, config.port)`.
    pub async fn new(config: Config) -> io::Result<Self> {
        ReadReplicaMap::new(config).await.map(ReadReplicaSet)
    }

    /// Provide the address of a known dated peer. See [`ReadReplicaMap::with_seed`].
    #[must_use]
    pub fn with_seed(self, peer: IpAddr) -> Self {
        ReadReplicaSet(self.0.with_seed(peer))
    }

    /// (runtime) Retune the probed network. See [`ReadReplicaMap::set_net`].
    pub fn set_net(&self, net: IpNet) {
        self.0.set_net(net);
    }

    /// Attach a dynamic peer-discovery source. See [`ReadReplicaMap::with_discovery`].
    #[must_use]
    pub fn with_discovery(self, discovery: Arc<dyn Discovery>) -> Self {
        ReadReplicaSet(self.0.with_discovery(discovery))
    }

    /// Discover peers by resolving a DNS name. See [`ReadReplicaMap::with_dns_discovery`].
    #[must_use]
    pub fn with_dns_discovery(self, name: impl Into<String>, port: u16) -> Self {
        ReadReplicaSet(self.0.with_dns_discovery(name, port))
    }

    /// Set how often the discovery task resolves the peer set. See
    /// [`ReadReplicaMap::with_discovery_interval`].
    #[must_use]
    pub fn with_discovery_interval(self, interval: Duration) -> Self {
        ReadReplicaSet(self.0.with_discovery_interval(interval))
    }

    /// The network this read replica currently probes. See [`ReadReplicaMap::net`].
    #[must_use]
    pub fn net(&self) -> IpNet {
        self.0.net()
    }

    /// Set the hook invoked before each inbound membership change. See
    /// [`ReadReplicaMap::set_on_update`].
    pub fn set_on_update<F: Send + Sync + Fn(&K, &State<()>) + 'static>(&self, on_update: F) {
        self.0.set_on_update(on_update);
    }

    /// Whether `key` is currently a member, as observed from the dated peer.
    #[must_use]
    pub fn contains(&self, key: &K) -> bool {
        self.0.contains_key(key)
    }

    /// Number of members currently held. See [`ReadReplicaMap::len`].
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the read replica holds no member. See [`ReadReplicaMap::is_empty`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Fingerprint over a key range. See [`ReadReplicaMap::value_fingerprint`].
    #[must_use]
    pub fn value_fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.0.value_fingerprint(range)
    }

    /// Deprecated alias for [`value_fingerprint`](Self::value_fingerprint) (#294).
    #[deprecated(since = "1.0.0", note = "renamed to `value_fingerprint`")]
    #[must_use]
    pub fn fingerprint<R: RangeBounds<K>>(&self, range: R) -> Fingerprint {
        self.value_fingerprint(range)
    }

    /// The current members. See [`ReadReplicaMap::keys`].
    #[must_use]
    pub fn keys(&self) -> Vec<K> {
        self.0.keys()
    }

    /// Start an out-of-cadence reconciliation round. See
    /// [`ReadReplicaMap::start_reconciliation`].
    pub async fn start_reconciliation(&self) {
        self.0.start_reconciliation().await;
    }

    /// Run the reconciliation loop. See [`ReadReplicaMap::run`].
    pub async fn run(self) {
        self.0.run().await;
    }
}

#[cfg(test)]
mod tests;
