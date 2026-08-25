// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::debug;

use crate::bounds::{Key, Value};
use crate::discovery::{Discovery, DnsDiscovery};

use super::ReadReplicaMap;

impl<K: Key, V: Value> ReadReplicaMap<K, V> {
    /// Attach a dynamic peer-discovery source (e.g. Kubernetes DNS), on top of the always-on
    /// random probe [`run`](Self::run) already sends each round.
    ///
    /// Unlike [`ReplicatedMap::with_discovery`](crate::ReplicatedMap::with_discovery), any
    /// [`Discovery`] implementation is accepted here regardless of
    /// [`kind`](Discovery::kind): a read replica holds no causal-stability membership and no GC
    /// gate a wrongly-decommissioned member could release (module docs), so the
    /// authoritative/speculative distinction that protects `ReplicatedMap` has nothing to protect
    /// here. Discovered addresses are seeded as gossip peers only, exactly like a peer learned by
    /// answering a probe, and age out the same way (60 s of silence).
    ///
    /// ```
    /// use std::sync::Arc;
    /// use reconcile::{replicated_map::Config, DnsDiscovery, ReadReplicaMap};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// // Point at a Kubernetes headless Service (`clusterIP: None`): one DNS record per ready pod.
    /// let discovery = Arc::new(DnsDiscovery::new("my-service.my-namespace.svc.cluster.local", 4242));
    /// let replica = ReadReplicaMap::<String, String>::new(Config::new(8085).with_insecure_no_key())
    ///     .await?
    ///     .with_discovery(discovery);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_discovery(mut self, discovery: Arc<dyn Discovery>) -> Self {
        self.discovery = Some(discovery);
        self
    }

    /// Discover peers by resolving a DNS name — [`with_discovery`](Self::with_discovery) with a
    /// [`DnsDiscovery`].
    ///
    /// Point `name` at a **headless** `Service` (`clusterIP: None`): one address record per ready
    /// pod, no API client and no RBAC.
    #[must_use]
    pub fn with_dns_discovery(self, name: impl Into<String>, port: u16) -> Self {
        self.with_discovery(Arc::new(DnsDiscovery::new(name, port)))
    }

    /// Set how often the discovery task resolves the peer set (default 5 s). Only relevant when a
    /// discovery source is configured via [`with_discovery`](Self::with_discovery).
    #[must_use]
    pub fn with_discovery_interval(mut self, interval: Duration) -> Self {
        self.discovery_interval = interval;
        self
    }

    /// Drive the dynamic discovery source: seed every resolved address as a known peer. A no-op
    /// with no source configured.
    ///
    /// Unlike [`ReplicatedMap::discover_periodically`](crate::ReplicatedMap), there is no
    /// decommissioning to perform: a read replica's peer set is not causal-stability membership,
    /// so an absent peer simply ages out of [`peers`](super::ReadReplicaMap) after 60 s of
    /// silence, same as a peer never rediscovered.
    pub(super) async fn discover_periodically(&self) {
        let Some(discovery) = self.discovery.clone() else {
            return; // no discovery source: leave peer-finding to the random probe alone
        };
        let own_addr = self.transport.local_addr().ok().map(|addr| addr.ip());
        loop {
            tokio::time::sleep(self.discovery_interval).await;
            let resolved = match discovery.discover().await {
                Ok(addrs) => addrs,
                Err(err) => {
                    // Transient failure: nothing to seed this round, nothing to age out early.
                    debug!("read replica discovery round failed, skipping: {err}");
                    continue;
                }
            };
            let now = Instant::now();
            let mut peers = self.peers.write();
            for addr in resolved {
                if Some(addr) == own_addr {
                    continue;
                }
                peers.insert(addr, now);
            }
        }
    }
}
