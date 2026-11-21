// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::bounds::{Key, Value};
use crate::discovery::{Discovery, DiscoveryKind, DnsDiscovery};
use crate::observability;

use super::ReplicatedMap;

/// Returned by [`with_discovery`](ReplicatedMap::with_discovery) when the supplied [`Discovery`]
/// source is not [`Authoritative`](DiscoveryKind::Authoritative).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub struct NotAuthoritative {
    /// The discovery source's actual [`DiscoveryKind`].
    pub kind: DiscoveryKind,
}

impl fmt::Display for NotAuthoritative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "with_discovery expects an authoritative source, got {:?}: a speculative prober \
             would be seeded as permanent known peers and its absences would wrongly decommission \
             members",
            self.kind
        )
    }
}

impl std::error::Error for NotAuthoritative {}

/// Per-member discovery-absence tracking for [`ReplicatedMap::discover_periodically`].
/// The miss counter resets to `Present` on any successful round that includes the peer.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum MemberPresence {
    #[default]
    Present,
    Absent { misses: u32 },
}

impl MemberPresence {
    /// Record that this member was present in the current discovery round.
    pub(super) fn mark_seen(&mut self) {
        *self = MemberPresence::Present;
    }

    /// Record a miss, incrementing the consecutive-miss counter.
    pub(super) fn mark_missed(&mut self) {
        *self = match *self {
            MemberPresence::Present => MemberPresence::Absent { misses: 1 },
            MemberPresence::Absent { misses } => MemberPresence::Absent {
                misses: misses.saturating_add(1),
            },
        };
    }

    /// Whether the consecutive-miss threshold has been reached.
    pub(super) fn eligible_for_decommission(&self, miss_threshold: u32) -> bool {
        matches!(self, MemberPresence::Absent { misses } if *misses >= miss_threshold)
    }
}

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// Attach an authoritative peer-discovery source.
    ///
    /// Successful rounds seed returned peers. Previously discovered members absent for the
    /// configured consecutive-miss threshold are decommissioned.
    ///
    /// # Errors
    ///
    /// Returns [`NotAuthoritative`] for speculative discovery sources.
    pub fn with_discovery(
        mut self,
        discovery: Arc<dyn Discovery>,
    ) -> Result<Self, NotAuthoritative> {
        let kind = discovery.kind();
        if !matches!(kind, DiscoveryKind::Authoritative) {
            return Err(NotAuthoritative { kind });
        }
        self.discovery = Some(discovery);
        Ok(self)
    }

    /// Discover peers by resolving a DNS name — [`with_discovery`](Self::with_discovery) with a
    /// [`DnsDiscovery`].
    /// Point `name` at a **headless** `Service` (`clusterIP: None`): one address record per ready
    /// pod, no API client and no RBAC.
    #[must_use]
    pub fn with_dns_discovery(self, name: impl Into<String>, port: u16) -> Self {
        // Infallible in practice: `DnsDiscovery::kind` always returns `Authoritative`,
        // unconditionally, so `with_discovery` can never actually reject it here.
        self.with_discovery(Arc::new(DnsDiscovery::new(name, port)))
            .expect("DnsDiscovery::kind() is always Authoritative")
    }

    /// Set how often the discovery task resolves the peer set (default 5 s). Only relevant when a
    /// discovery source is configured via [`with_discovery`](Self::with_discovery).
    pub fn with_discovery_interval(mut self, interval: Duration) -> Self {
        self.discovery_interval = interval;
        self
    }

    /// Set how many consecutive successful discovery rounds a seen member may be absent
    /// before it is decommissioned (default 3). A higher value tolerates longer DNS blips / rolling
    /// restarts at the cost of retaining causal membership and its ACK obligations longer.
    pub fn with_discovery_miss_threshold(mut self, threshold: u32) -> Self {
        self.discovery_miss_threshold = threshold;
        self
    }

    /// Drive the configured discovery source.
    ///
    /// Successful rounds refresh known peers and count absences for authoritative members. Failed
    /// rounds do not count as misses. Only members previously observed through this source are
    /// eligible for decommissioning.
    pub(super) async fn discover_periodically(&self) {
        let Some(discovery) = self.discovery.clone() else {
            return; // no discovery source: leave peer-finding to the engine's per-net probing
        };
        let own_addr = self.engine.listen_addr();
        // Presence state per address discovery has ever reported (so we only grace-decommission
        // members we actually discovered, never peers learned by other means).
        let mut presence: HashMap<IpAddr, MemberPresence> = HashMap::new();
        loop {
            tokio::time::sleep(self.discovery_interval).await;
            let resolved = match discovery.discover().await {
                Ok(addrs) => {
                    *self.last_successful_discovery_at.write() = Some(Instant::now());
                    addrs
                }
                Err(err) => {
                    // Transient failure: do not touch presence state, do not decommission anyone.
                    observability::record_discovery_failure();
                    warn!("discovery round failed, skipping: {err}");
                    continue;
                }
            };
            let current: HashSet<IpAddr> = resolved
                .into_iter()
                .filter(|addr| *addr != own_addr)
                .collect();
            // 1) Refresh every currently-present peer.
            for addr in &current {
                presence.entry(*addr).or_default().mark_seen();
                self.engine.seed_peer(*addr);
            }
            // 2) Grace-account members that were discovered before but are now absent.
            for member in self.engine.members_snapshot() {
                if member == own_addr || current.contains(&member) {
                    continue;
                }
                let Some(state) = presence.get_mut(&member) else {
                    continue; // never discovered by this source: not ours to decommission
                };
                state.mark_missed();
                if state.eligible_for_decommission(self.discovery_miss_threshold) {
                    info!("decommissioning vanished peer {member}");
                    self.engine.decommission_peer(member);
                    presence.remove(&member);
                }
            }
        }
    }
}
