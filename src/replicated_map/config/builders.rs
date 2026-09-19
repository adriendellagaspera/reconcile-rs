// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;
use std::time::Duration;

use gossip::auth::{ClusterKey, Keys};
use ipnet::IpNet;

use crate::clock::{ClockDrift, NodeId};

use super::{Config, ConfigError};

impl Config {
    /// The documented default constructor: `port` is the one setting every node in a cluster
    /// must agree on (see [`port`](Self::port)'s docs for why `0` can never converge). Equivalent
    /// to `Config::default().with_port(port)`.
    #[must_use]
    pub fn new(port: u16) -> Self {
        Config::default().with_port(port)
    }

    /// Set [`port`](Self::port).
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    /// Set [`listen_addr`](Self::listen_addr).
    #[must_use]
    pub fn with_listen_addr(mut self, listen_addr: IpAddr) -> Self {
        self.listen_addr = listen_addr;
        self
    }
    /// Declare a geographical network by its CIDR — once per network, **including this node's
    /// own** (see [`nets`](Config::nets)).
    ///
    /// # Errors
    ///
    /// If more than [`MAX_NETS`](super::MAX_NETS) networks are declared — the same
    /// [`MAX_NETS`](super::MAX_NETS) cap
    /// [`ReplicatedMap::set_nets`](super::super::ReplicatedMap::set_nets)/
    /// [`add_net`](super::super::ReplicatedMap::add_net) enforce at runtime.
    pub fn with_net(mut self, net: IpNet) -> Result<Self, ConfigError> {
        let slot = self
            .nets
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(ConfigError::TooManyNets)?;
        *slot = Some(net);
        Ok(self)
    }

    /// Declare several networks at once (see [`with_net`](Config::with_net)).
    ///
    /// # Errors
    ///
    /// If the total exceeds [`MAX_NETS`](super::MAX_NETS).
    pub fn with_nets(mut self, nets: &[IpNet]) -> Result<Self, ConfigError> {
        for &net in nets {
            self = self.with_net(net)?;
        }
        Ok(self)
    }

    /// Set how often (in reconciliation rounds) the full anti-entropy comparison is sent to
    /// remote-network peers (default `6`). See [`remote_interval`](Config::remote_interval).
    #[must_use]
    pub fn with_remote_interval(mut self, interval: u32) -> Self {
        self.remote_interval = interval;
        self
    }

    /// Set the maximum number of peers contacted per remote network on each cross-network round
    /// (default `2`). See [`remote_fanout`](Config::remote_fanout).
    #[must_use]
    pub fn with_remote_fanout(mut self, fanout: usize) -> Self {
        self.remote_fanout = fanout;
        self
    }

    /// Set the reconciliation cadence: how long the loop waits for inbound activity before
    /// initiating a round (default 1 s). See [`reconcile_interval`](Config::reconcile_interval).
    /// Retunable at runtime via
    /// [`ReplicatedMap::set_reconcile_interval`](crate::ReplicatedMap::set_reconcile_interval).
    #[must_use]
    pub fn with_reconcile_interval(mut self, interval: Duration) -> Self {
        self.reconcile_interval = interval;
        self
    }

    /// Set the RTT-scale repair timer (default 150 ms). See
    /// [`repair_interval`](Config::repair_interval). Retunable at runtime via
    /// [`ReplicatedMap::set_repair_interval`](crate::ReplicatedMap::set_repair_interval).
    #[must_use]
    pub fn with_repair_interval(mut self, interval: Duration) -> Self {
        self.repair_interval = interval;
        self
    }

    /// Set the rate, in bytes per second, at which a single bulk anti-entropy value transfer to one
    /// peer is paced (default 32 MiB/s). See [`bulk_send_rate`](Config::bulk_send_rate); to disable
    /// pacing (an unpaced back-to-back burst), set that field to `None` directly.
    #[must_use]
    pub fn with_bulk_send_rate(mut self, bytes_per_sec: usize) -> Self {
        self.bulk_send_rate = Some(bytes_per_sec);
        self
    }

    /// Request `size` bytes for `SO_RCVBUF`; the kernel clamps to the OS maximum. Raising this
    /// with the matching sysctl is the fix for datagrams dropped during a cold sync. Set
    /// [`recv_buffer_size`](Config::recv_buffer_size) to `None` for the OS default.
    #[must_use]
    pub fn with_recv_buffer_size(mut self, size: usize) -> Self {
        self.recv_buffer_size = Some(size);
        self
    }

    /// Request `size` bytes for `SO_SNDBUF`; the kernel clamps to the OS maximum. Set
    /// [`send_buffer_size`](Config::send_buffer_size) to `None` for the OS default.
    #[must_use]
    pub fn with_send_buffer_size(mut self, size: usize) -> Self {
        self.send_buffer_size = Some(size);
        self
    }

    /// Enable per-datagram MAC authentication with one shared cluster secret.
    ///
    /// Incoming datagrams are verified before deserialization and silently dropped on failure.
    /// Every node must share the key and MAC backend (`mac-blake3` or `mac-hmac`) outside a key
    /// rotation. Calling this also closes any receive-side rotation window opened by
    /// [`with_cluster_key_rotation`](Self::with_cluster_key_rotation).
    #[must_use]
    pub fn with_cluster_key(mut self, key: ClusterKey) -> Self {
        self.cluster_key = Some(key);
        self.rotation_key = None;
        self
    }

    /// Open a two-key rotation window: seal outgoing datagrams with `primary`, while accepting
    /// incoming datagrams authenticated by either `primary` or `also_accept`.
    ///
    /// Rotate in three cluster-wide phases, completing each rollout before starting the next:
    /// `old + accept(new)` → `new + accept(old)` → [`with_cluster_key(new)`](Self::with_cluster_key).
    /// This changes no wire bytes; the receiver simply tries both secrets. See README
    /// "Cluster-key rotation" for provisioning and the temporary keyed-fingerprint cost.
    #[must_use]
    pub fn with_cluster_key_rotation(
        mut self,
        primary: ClusterKey,
        also_accept: ClusterKey,
    ) -> Self {
        self.cluster_key = Some(primary);
        self.rotation_key = Some(also_accept);
        self
    }

    /// The authentication key set implied by the public config: one primary plus at most one
    /// receive-only fallback. The primary is cloned rather than moved because callers still need
    /// it to derive the RSOS lift key during construction.
    pub(crate) fn auth_keys(&self) -> Option<Keys> {
        self.cluster_key.clone().map(|primary| Keys {
            primary,
            also_accept: self.rotation_key.clone().into_iter().collect(),
        })
    }

    /// Explicit, loudly-named opt-in to run with no [`cluster_key`](Self::cluster_key) at all.
    ///
    /// Without either this or [`with_cluster_key`](Self::with_cluster_key), construction refuses
    /// to proceed: [`RandomProbe`](crate::discovery::RandomProbe) answers any host inside the
    /// configured [`nets`](Self::nets), so a stranger squatting one IP eventually receives the
    /// **entire dataset**, unauthenticated, via paced diff dumps. Call this only when the network
    /// is a trusted underlay the cluster fully controls; `SECURITY.md` is canonical for the
    /// trust boundary.
    #[must_use]
    pub fn with_insecure_no_key(mut self) -> Self {
        self.insecure_no_key = true;
        self
    }

    /// `cluster_key: None` without the explicit `insecure_no_key` opt-in is a
    /// construction-time error, not a silent unauthenticated run. Shared by every engine
    /// constructor (`Replica`, `ReadReplicaMap`) so none of them can bypass it.
    pub(crate) fn check_key_or_insecure_opt_in(&self) -> Result<(), ConfigError> {
        if self.cluster_key.is_some() || self.insecure_no_key {
            Ok(())
        } else {
            Err(ConfigError::MissingSecurityMode)
        }
    }

    /// Set an explicit node identity for the HLC tie-break; must be distinct per node.
    #[must_use]
    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Set [`freshness_window`](Config::freshness_window). No effect unkeyed.
    #[must_use]
    pub fn with_freshness_window(mut self, window: Duration) -> Self {
        self.freshness_window = window;
        self
    }

    /// Set the maximum number of distinct peers tracked (default 1024). See
    /// [`max_peers`](Config::max_peers).
    #[must_use]
    pub fn with_max_peers(mut self, max: usize) -> Self {
        self.max_peers = max;
        self
    }

    /// Set [`max_concurrent_bulk_dumps`](Config::max_concurrent_bulk_dumps) (default 4).
    #[must_use]
    pub fn with_max_concurrent_bulk_dumps(mut self, max: usize) -> Self {
        self.max_concurrent_bulk_dumps = max;
        self
    }

    /// Set [`max_concurrent_broadcasts`](Config::max_concurrent_broadcasts) (default 1024).
    #[must_use]
    pub fn with_max_concurrent_broadcasts(mut self, max: usize) -> Self {
        self.max_concurrent_broadcasts = max;
        self
    }

    /// Set [`snapshot_interval`](Config::snapshot_interval) (default `Some(5 s)`). `None`
    /// disables the periodic background snapshot task entirely — only an explicit
    /// [`ReplicatedMap::snapshot_now`](super::super::ReplicatedMap::snapshot_now) call writes a
    /// snapshot from then on.
    #[must_use]
    pub fn with_snapshot_interval(mut self, interval: Option<Duration>) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Set [`snapshot_change_threshold`](Config::snapshot_change_threshold) (default `1`).
    #[must_use]
    pub fn with_snapshot_change_threshold(mut self, threshold: usize) -> Self {
        self.snapshot_change_threshold = threshold;
        self
    }

    /// Set [`max_clock_drift`](Config::max_clock_drift) (default
    /// [`MAX_CLOCK_DRIFT`](crate::clock::MAX_CLOCK_DRIFT)).
    #[must_use]
    pub fn with_max_clock_drift(mut self, drift: ClockDrift) -> Self {
        self.max_clock_drift = drift;
        self
    }

    /// Set [`coalesce_window`](Config::coalesce_window) (default [`Duration::ZERO`], i.e. no
    /// coalescing). Retunable at runtime via
    /// [`ReplicatedMap::set_coalesce_window`](crate::ReplicatedMap::set_coalesce_window).
    #[must_use]
    pub fn with_coalesce_window(mut self, window: Duration) -> Self {
        self.coalesce_window = window;
        self
    }

    /// Set [`max_value_size`](Config::max_value_size) (default `None`, no ceiling). Checked only
    /// by [`ReplicatedMap::try_insert`](crate::ReplicatedMap::try_insert)/
    /// [`try_update`](crate::ReplicatedMap::try_update) —
    /// [`insert`](crate::ReplicatedMap::insert)/[`update`](crate::ReplicatedMap::update) are
    /// unaffected either way.
    #[must_use]
    pub fn with_max_value_size(mut self, max: usize) -> Self {
        self.max_value_size = Some(max);
        self
    }

    /// Encrypt datagram payloads with XChaCha20-Poly1305, reusing
    /// [`cluster_key`](Self::cluster_key) as the AEAD key — so
    /// [`with_cluster_key`](Self::with_cluster_key) or
    /// [`with_cluster_key_rotation`](Self::with_cluster_key_rotation) is required on every node.
    ///
    /// Framed as `nonce || ciphertext || tag`, 40 bytes of overhead, verified before
    /// deserialization. The trust model is unchanged: one shared secret at a time for sending, so
    /// no per-peer identity and no forward secrecy.
    ///
    /// Requires the `encryption` cargo feature.
    #[cfg(feature = "encryption")]
    #[must_use]
    pub fn with_encryption(mut self) -> Self {
        self.encrypt = true;
        self
    }
}
