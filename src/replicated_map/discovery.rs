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
/// [`Absent`](Self::Absent) owns the miss counter and the instant the absence began as one unit,
/// so the two cannot desync.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum MemberPresence {
    #[default]
    Present,
    Absent {
        since: Instant,
        misses: u32,
    },
}

impl MemberPresence {
    /// Record that this member was present in the current discovery round.
    pub(super) fn mark_seen(&mut self) {
        *self = MemberPresence::Present;
    }

    /// Record that this member was missing from the current discovery round, starting the absence
    /// clock on the first miss and incrementing the counter on every subsequent one.
    pub(super) fn mark_missed(&mut self) {
        *self = match *self {
            MemberPresence::Present => MemberPresence::Absent {
                since: Instant::now(),
                misses: 1,
            },
            MemberPresence::Absent { since, misses } => MemberPresence::Absent {
                since,
                misses: misses + 1,
            },
        };
    }

    /// Whether this absence warrants decommissioning: at `miss_threshold` misses, immediately
    /// without a pending unacknowledged tombstone, otherwise only past `floor` — which is what
    /// keeps a flaky resolver from releasing the GC gate early.
    pub(super) fn eligible_for_decommission(
        &self,
        miss_threshold: u32,
        floor: Duration,
        pending_tombstone_acks: bool,
    ) -> bool {
        let MemberPresence::Absent { since, misses } = *self else {
            return false;
        };
        if misses < miss_threshold {
            return false;
        }
        !pending_tombstone_acks || since.elapsed() >= floor
    }
}

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// Attach an authoritative peer-discovery source.
    ///
    /// Successful rounds seed returned peers. Members absent for the configured miss threshold
    /// are decommissioned according to the tombstone-safety floor.
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
    /// restarts at the cost of holding tombstones (and their GC gate) longer.
    pub fn with_discovery_miss_threshold(mut self, threshold: u32) -> Self {
        self.discovery_miss_threshold = threshold;
        self
    }

    /// Set the wall-time floor a member **with pending unacknowledged tombstones** must be
    /// continuously absent for before decommissioning (default 10 minutes).
    /// The fast path — no pending acks — is unaffected. The floor is what keeps a spoofed or
    /// flaky resolver from releasing the GC gate on a tombstone a healthy member never acked, and
    /// so from letting that member resurrect the value. Raising it bounds the attacker further and
    /// holds tombstones longer during a genuine outage.
    pub fn with_discovery_decommission_floor(mut self, floor: Duration) -> Self {
        self.discovery_decommission_floor = floor;
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
                let pending = self.engine.has_pending_tombstone_acks(member);
                if state.eligible_for_decommission(
                    self.discovery_miss_threshold,
                    self.discovery_decommission_floor,
                    pending,
                ) {
                    info!(
                        "decommissioning vanished peer {member} \
                         (pending_tombstone_acks={pending})"
                    );
                    self.engine.decommission_peer(member);
                    presence.remove(&member);
                }
            }
        }
    }
}
