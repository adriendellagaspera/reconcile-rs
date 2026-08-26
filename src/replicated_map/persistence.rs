// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::hash::Hash;
use std::io;
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::warn;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::Entry;
use crate::observability;
use crate::persistence::{DatedEntries, PersistedState, Persistence};

use super::ReplicatedMap;

/// How often the background task writes a full snapshot to the persistence backend.
pub(super) const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5);

/// Attempts [`with_persistence`](ReplicatedMap::with_persistence) makes to load persisted state
/// before giving up.
pub(super) const LOAD_RETRY_ATTEMPTS: u32 = 5;

/// Base delay before the first load retry; each subsequent attempt doubles it (see
/// [`backoff_delay`]) — 100 ms, 200 ms, 400 ms, 800 ms, under 2 s of total backoff across
/// [`LOAD_RETRY_ATTEMPTS`].
pub(super) const LOAD_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// Delay before retry `attempt` (1-indexed): `LOAD_RETRY_BASE_DELAY` doubled `attempt - 1` times.
pub(super) fn backoff_delay(attempt: u32) -> Duration {
    LOAD_RETRY_BASE_DELAY * 2u32.pow(attempt - 1)
}

/// Entries cloned per `Arc` snapshot re-load while building a persisted snapshot
/// (`Self::persist_snapshot`).
///
/// Predates #34's move from `RwLock` to `ArcSwap`: chunking used to bound how long one
/// continuous read-lock acquisition could stall a writer. A writer never blocks on a reader under
/// `ArcSwap` at all, so that motivation is gone, but the resulting snapshot is still not a single
/// linearizable instant — a fresh `load_full()` between chunks can observe a write concurrent
/// with an earlier chunk — which is no different from what the gossip protocol itself already
/// reconciles range-by-range, and each individual entry is still read atomically
/// (`ARCHITECTURE.md` §5 invariant 8's per-key LWW model needs no more).
pub(super) const SNAPSHOT_CHUNK_SIZE: usize = 4096;

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// Plug in a durable persistence backend, **loading any previously saved state first**.
    ///
    /// Call between [`new`](ReplicatedMap::new) and [`run`](ReplicatedMap::run), so entries,
    /// tombstones and the causal-stability membership are recovered before the node rejoins gossip.
    /// Loaded entries replay through the pre-insert hook, preserving each tombstone's deletion
    /// timestamp and rebuilding the expiry wheel.
    ///
    /// # Panics
    ///
    /// If the backend fails to load: a damaged durable state must be an explicit decision, never a
    /// silent fresh start. A *transient* failure (anything other than
    /// [`InvalidData`](io::ErrorKind::InvalidData) — a not-yet-mounted volume, a momentary
    /// permission or I/O hiccup) is retried up to `LOAD_RETRY_ATTEMPTS` (5) times with exponential
    /// backoff before this panics, so a slow-starting environment does not crash-loop on every
    /// restart attempt; a decode/format error ([`InvalidData`](io::ErrorKind::InvalidData)) is
    /// never transient and panics immediately, unretried.
    ///
    /// ```
    /// use reconcile::{
    ///     replicated_map::Config, Entry, FileSnapshot, Hlc, LogicalCounter, NodeId,
    ///     PersistedState, Persistence, PhysicalTime, ReplicatedMap, Timestamp,
    /// };
    /// use std::sync::Arc;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> std::io::Result<()> {
    /// let dir = tempfile::tempdir()?;
    /// let backend = FileSnapshot::new(dir.path().join("snapshot"));
    ///
    /// // Simulates a prior process that shut down having persisted one entry.
    /// let stamp = Timestamp::new(Hlc::new(PhysicalTime::from_millis(0), LogicalCounter::new(0)), NodeId::new(1));
    /// backend.save(&PersistedState::from(vec![("a".to_string(), Entry::present(stamp, 1))]))?;
    ///
    /// // A fresh store, pointed at that same backend, recovers the entry immediately -- loading
    /// // happens synchronously in `with_persistence` itself, not on the periodic save timer.
    /// let store = ReplicatedMap::<String, i32>::new(Config::new(8085).with_insecure_no_key())
    ///     .await?
    ///     .with_persistence(Arc::new(backend));
    /// assert_eq!(store.get_cloned(&"a".to_string()), Some(1));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_persistence(mut self, backend: Arc<dyn Persistence<K, V>>) -> Self {
        // A random node id changes every restart, so the LWW tie-break is stable only within one
        // process lifetime — durable state wants an explicit `Config::with_node_id`.
        if self.engine.node_id_is_random() {
            warn!(
                "persistence is enabled but no stable node_id was configured \
                 (Config::with_node_id was not called). The node id is randomly generated on \
                 every start, so this node's LWW conflict-resolution identity changes across \
                 restarts. Conflicts between a pre-restart write and a post-restart write from \
                 the same node are resolved non-deterministically. Set a stable, unique \
                 Config::with_node_id to preserve consistent LWW ordering across restarts."
            );
        }
        let loaded = {
            let mut attempt = 0u32;
            loop {
                match backend.load() {
                    Ok(state) => break state,
                    Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                        panic!("persisted state is corrupt or from an incompatible format, refusing to silently start fresh: {err}");
                    }
                    Err(err) if attempt + 1 < LOAD_RETRY_ATTEMPTS => {
                        attempt += 1;
                        let delay = backoff_delay(attempt);
                        warn!(
                            "transient failure loading persisted state (attempt {attempt}/{LOAD_RETRY_ATTEMPTS}): \
                             {err}; retrying in {delay:?}"
                        );
                        std::thread::sleep(delay);
                    }
                    Err(err) => {
                        panic!(
                            "failed to load persisted state after {LOAD_RETRY_ATTEMPTS} attempts: {err}"
                        );
                    }
                }
            }
        };
        if let Some(state) = loaded {
            *self.engine.members.write() = state.members;
            *self.engine.tombstone_acks.write() = state.tombstone_acks;
            // Advance past every persisted stamp, or a fresh write can lose LWW to this node's
            // own older value after a backward clock step. Trusted path: these stamps are
            // self-authored, and the clamp would refuse to chase them in exactly that scenario.
            for (_, entry) in &state.entries {
                self.engine.clock_observe_trusted(entry.stamp);
            }
            // Replay through the wrapped hook: the public insert helpers would re-stamp.
            self.engine.just_insert_bulk(&state.entries);
        }
        self.persistence = backend;
        self
    }

    /// Register a callback invoked with the [`io::Error`] whenever a snapshot write to the
    /// persistence backend fails — both the periodic background snapshot and a caller-triggered
    /// [`snapshot_now`](Self::snapshot_now).
    ///
    /// Runs in addition to, never instead of, the `reconcile_persistence_failures_total` counter
    /// (behind the `metrics` feature) and the `warn!` already logged on the periodic path — this
    /// is for an operator's own alerting (e.g. paging on the *first* failure rather than waiting
    /// on a counter to cross a threshold), not a replacement for either. A second call replaces
    /// the first, it does not add to it.
    pub fn on_persistence_error<F: Fn(&io::Error) + Send + Sync + 'static>(
        mut self,
        hook: F,
    ) -> Self {
        self.persistence_error_hook = Arc::new(hook);
        self
    }

    /// Capture the full store state and hand it to the persistence backend.
    ///
    /// Clones the map in [`SNAPSHOT_CHUNK_SIZE`]-entry chunks — see that constant's doc for why a
    /// non-instantaneous snapshot is an acceptable trade-off here. Records
    /// [`last_snapshot_at`](super::ReplicatedMap::sync_state) on success, and the
    /// `reconcile_persistence_failures_total`/`reconcile_persistence_failures_current` metrics
    /// (behind the `metrics` feature) plus [`on_persistence_error`](Self::on_persistence_error) on
    /// failure.
    fn snapshot_inner(&self) -> io::Result<()> {
        let mut entries: DatedEntries<K, V> = Vec::new();
        let mut cursor: Option<K> = None;
        loop {
            let guard = self.engine.map.load_full();
            let chunk: Vec<(K, Entry<Timestamp, V>)> = match &cursor {
                None => guard
                    .range(..)
                    .take(SNAPSHOT_CHUNK_SIZE)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                Some(last) => guard
                    .range((Bound::Excluded(last.clone()), Bound::Unbounded))
                    .take(SNAPSHOT_CHUNK_SIZE)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            drop(guard);
            let Some((last_key, _)) = chunk.last() else {
                break;
            };
            cursor = Some(last_key.clone());
            entries.extend(chunk);
        }
        let state = PersistedState::new(
            entries,
            self.engine.members.read().clone(),
            self.engine.tombstone_acks.read().clone(),
        );
        match self.persistence.save(&state) {
            Ok(()) => {
                *self.last_snapshot_at.write() = Some(Instant::now());
                self.persistence_consecutive_failures
                    .store(0, Ordering::Relaxed);
                observability::record_persistence_success();
                Ok(())
            }
            Err(err) => {
                let consecutive = self
                    .persistence_consecutive_failures
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                observability::record_persistence_failure(consecutive);
                (self.persistence_error_hook)(&err);
                Err(err)
            }
        }
    }

    /// As [`snapshot_inner`](Self::snapshot_inner), logging rather than propagating a failure —
    /// the shape the periodic background task and [`run`](super::ReplicatedMap::run)'s shutdown
    /// flush both want.
    ///
    /// Named `persist_snapshot`, not `snapshot` (#34): that name now belongs to the public,
    /// in-memory `Arc` snapshot ([`ReplicatedMap::snapshot`](super::ReplicatedMap::snapshot)) —
    /// an unrelated concept that happens to share the word.
    pub(super) fn persist_snapshot(&self) {
        if let Err(err) = self.snapshot_inner() {
            warn!("failed to persist reconcile store snapshot: {err}");
        }
    }

    /// Force an out-of-band snapshot right now, rather than waiting for the periodic background
    /// task (cadence: [`Config::snapshot_interval`](super::Config::snapshot_interval)).
    ///
    /// # Errors
    ///
    /// If the persistence backend's [`save`](Persistence::save) fails. Unlike the periodic task
    /// (which only logs a failure and keeps running), a caller-triggered flush hands the error
    /// back, since silently swallowing it here would leave a caller with no signal that the
    /// snapshot they explicitly asked for did not happen.
    pub fn snapshot_now(&self) -> io::Result<()> {
        self.snapshot_inner()
    }

    /// Periodically snapshot the full store state to the persistence backend.
    pub(super) async fn snapshot_periodically(&self) {
        loop {
            tokio::time::sleep(self.snapshot_interval).await;
            self.persist_snapshot();
        }
    }
}
