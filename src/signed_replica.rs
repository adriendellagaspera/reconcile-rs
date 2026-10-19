// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Opt-in, signed-only, substrate-neutral authoritative replica state machine.
//!
//! This is a separate v0.x constructor path, not an IP-to-identity adapter. It accepts only
//! allowlisted Ed25519 principals with a verified return route, stores causal memberships and
//! tombstone acknowledgements under stable `PeerId`, and saves the version-2 durable format.
//! The caller drives datagram I/O using any substrate address `A` and calls `reconcile` on its
//! preferred cadence. The existing `ReplicatedMap` legacy IP runtime remains unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gossip::auth::Authenticator;
use gossip::framing::Reassembler;
use gossip::peer::identity::{PeerAllowlist, VerifiedPeerId};
use gossip::peer::routes::SignedPeerRoutes;
use gossip::peer::runtime::SignedDatagramBoundary;
use gossip::replay::FRESHNESS_WINDOW_DEFAULT;

use crate::bounds::{Key, Value};
use crate::clock::{Clock, Timestamp};
use crate::entry::Entry;
use crate::persistence::{PersistedState, Persistence};
use crate::replica::{GenerationTracker, RecoveryBook};
use crate::replicated_map::FramingConfig;
use crate::{ClusterKey, PeerId};

/// A node-specific signing key, separate from the cluster MAC key and the HLC node ID.
pub use gossip::peer::identity::PeerSigner;
/// Complete signed outbound bytes paired with an opaque substrate destination.
pub use gossip::peer::routes::RoutedDatagram;

/// Explicit roles and bounded framing for a signed-only replication cluster.
///
/// `authorities` and `readers` must come from trusted provisioning, never from incoming
/// packets. A value-only reader can retrieve updates but cannot enter another node's causal
/// membership or gate its tombstone GC, even if it sends a forged dated-channel message.
#[derive(Clone, Debug)]
pub struct SignedConfig {
    /// Independently authorized full peers, excluding the local signer.
    pub authorities: HashSet<PeerId>,
    /// Authorized value-only readers, excluded from causal membership.
    pub readers: HashSet<PeerId>,
    /// Whether this local node is a value-only reader and cannot originate writes or ACKs.
    pub read_only: bool,
    /// Maximum active authoritative causal members.
    pub max_peers: usize,
    /// Independent capacity for the per-identity signed replay window.
    pub max_replay_senders: usize,
    /// Reject signed datagrams outside this freshness window.
    pub freshness_window: Duration,
    /// Lifetime of a return-route challenge before it expires.
    pub challenge_ttl: Duration,
    /// Fragment, sender-recovery, memory, logical-message and substrate-MTU bounds.
    pub framing: FramingConfig,
}

impl SignedConfig {
    /// Provision a signed-only cluster with explicit authoritative public keys.
    ///
    /// No addresses or IP-to-identity conversions are accepted here.
    pub fn new(authorities: impl IntoIterator<Item = PeerId>) -> Self {
        Self {
            authorities: authorities.into_iter().collect(),
            readers: HashSet::new(),
            read_only: false,
            max_peers: 256,
            max_replay_senders: 512,
            freshness_window: FRESHNESS_WINDOW_DEFAULT,
            challenge_ttl: Duration::from_secs(5),
            framing: FramingConfig::default(),
        }
    }
}

/// A single-writer signed LWW replica with verified-principal causal stability.
///
/// This is an I/O-independent actor: `bootstrap` and `reconcile` return signed datagrams
/// for the caller's physical transport, and `receive` processes one incoming datagram. All
/// application messages are authenticated, versioned, replay-protected and route-confirmed.
/// The retained dataset fits in memory on each authoritative node. Writes are eventually
/// broadcast during reconciliation; callers must regularly drive that method.
///
/// Persisted identities use `Persistence<K, V, PeerId>` (disk schema v2), never the historical
/// IP schema v1. Loading happens synchronously before any packet is processed.
pub struct SignedReplica<K, V, A>
where
    K: Key + Hash,
    V: Value,
    A: Clone + Eq,
{
    boundary: SignedDatagramBoundary,
    routes: SignedPeerRoutes<A>,
    reassembler: Reassembler<VerifiedPeerId>,
    recovery: RecoveryBook<VerifiedPeerId>,
    entries: BTreeMap<K, Entry<Timestamp, V>>,
    members: HashSet<PeerId>,
    tombstone_acks: HashMap<K, HashMap<PeerId, u64>>,
    generations: GenerationTracker<K, V, PeerId>,
    persistence: Arc<dyn Persistence<K, V, PeerId>>,
    clock: Arc<dyn Clock>,
    authorities: HashSet<PeerId>,
    readers: HashSet<PeerId>,
    read_only: bool,
    framing: FramingConfig,
    outbound_cursors: HashMap<PeerId, Option<K>>,
    next_peer_index: usize,
    max_peer_cap: usize,
}

impl<K, V, A> SignedReplica<K, V, A>
where
    K: Key + Hash,
    V: Value,
    A: Clone + Eq,
{
    /// Construct a signed-only replica and load all causal state before network admission.
    ///
    /// # Errors
    /// Returns an explicit error for invalid crypto/MTU configuration, incompatible v1
    /// snapshots, corrupted v2 data, unrecognized retained members, or backend load failure.
    /// No incompatible durable state is ever replaced with an empty cluster.
    pub fn new(
        config: SignedConfig,
        cluster_key: ClusterKey,
        signer: PeerSigner,
        clock: Arc<dyn Clock>,
        persistence: Arc<dyn Persistence<K, V, PeerId>>,
    ) -> io::Result<Self> {
        let local = signer.peer_id();
        if config.authorities.contains(&local)
            || config.readers.contains(&local)
            || !config.authorities.is_disjoint(&config.readers)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "signed local and remote authoritative/read-only identities must be disjoint",
            ));
        }
        if config.max_peers < config.authorities.len()
            || config.max_replay_senders < config.authorities.len() + config.readers.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "signed peer and replay limits must accommodate the provisioned allowlist",
            ));
        }
        let authenticator = Authenticator::new(Some(cluster_key), false)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, format!("{err:?}")))?;
        let allowlist =
            PeerAllowlist::new(config.authorities.iter().chain(&config.readers).copied());
        let boundary = SignedDatagramBoundary::new(
            authenticator,
            signer,
            allowlist,
            config.freshness_window,
            config.max_replay_senders,
            config.max_peers,
            config.framing.datagram_payload_budget,
        )
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, format!("{err:?}")))?;
        if boundary.payload_budget() <= gossip::framing::FRAGMENT_HEADER_LEN
            || boundary.payload_budget() < 65
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "signed datagram budget cannot carry route proof or a nonempty fragment",
            ));
        }
        if config.framing.max_logical_message_size > u32::MAX as usize
            || config.framing.max_fragments_per_message == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid signed logical-message or fragment limit",
            ));
        }

        let loaded = persistence.load()?;
        let had_snapshot = loaded.is_some();
        let saved = loaded.unwrap_or_default();
        if had_snapshot
            && (if config.read_only {
                !saved.members.is_empty() || !saved.tombstone_acks.is_empty()
            } else {
                saved.members != config.authorities
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "signed durable membership differs from provisioned authorities; an explicit coordinated reset or persisted decommission is required",
            ));
        }
        if !saved.members.is_subset(&config.authorities)
            || saved
                .tombstone_acks
                .values()
                .any(|acks| acks.keys().any(|peer| !config.authorities.contains(peer)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "persisted causal blockers include identities not in the authoritative allowlist; decommission explicitly before changing roster",
            ));
        }
        let mut entries = BTreeMap::new();
        for (key, entry) in saved.entries {
            clock.observe_trusted(entry.stamp);
            if entries.insert(key, entry).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "persisted signed snapshot contains duplicate keys",
                ));
            }
        }
        // Every configured authority is a causal blocker from the outset, even if it has
        // never been reachable. Otherwise an offline member could later resurrect an
        // already-collected deletion. A read-only replica has no causal members.
        let members = if config.read_only {
            HashSet::new()
        } else {
            config.authorities.clone()
        };
        let generations = GenerationTracker::default();
        if !had_snapshot {
            let mut gen = generations.mutation();
            for id in &members {
                gen.record_member(*id, true);
            }
        }
        Ok(Self {
            boundary,
            routes: SignedPeerRoutes::new(config.max_peers, config.challenge_ttl),
            reassembler: Reassembler::for_identity(config.framing.reassembly_limits()),
            recovery: RecoveryBook::for_identity(),
            entries,
            members,
            tombstone_acks: saved.tombstone_acks,
            generations,
            persistence,
            clock,
            authorities: config.authorities,
            readers: config.readers,
            read_only: config.read_only,
            framing: config.framing,
            outbound_cursors: HashMap::new(),
            next_peer_index: 0,
            max_peer_cap: config.max_peers,
        })
    }

    /// This node's stable, cryptographically backed logical identity.
    pub fn peer_id(&self) -> PeerId {
        self.boundary.local_peer()
    }

    /// A clone of the monotonic, authoritative, authenticated causal membership.
    pub fn members(&self) -> HashSet<PeerId> {
        self.members.clone()
    }

    /// Inspect a local live value (tombstones return `None`).
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).and_then(Entry::value)
    }

    /// Test whether a key is currently held as a tombstone, independently of GC eligibility.
    pub fn is_tombstone(&self, key: &K) -> bool {
        self.entries.get(key).is_some_and(Entry::is_tombstone)
    }

    /// Number of retained dated records, including not-yet-collected tombstones.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no records are retained.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

mod causal;
mod network;

#[cfg(test)]
mod tests;
