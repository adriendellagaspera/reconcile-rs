// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Bounded, hint-only discovery independent of signed causal membership.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::time::{Duration, Instant};

use super::DiscoveryError;

/// The object-safe resolver future for opaque substrate endpoints.
pub type EndpointDiscoveryFuture<'a, A> =
    Pin<Box<dyn Future<Output = Result<Vec<A>, DiscoveryError>> + Send + 'a>>;

/// Resolve untrusted endpoint hints without granting peer identity or membership.
///
/// Implementations must bound their own allocation and return at most `limit` raw hints,
/// including duplicates. The consumer rejects oversized results and bounds how long it waits.
/// Resolver construction must return promptly and its future must yield cooperatively; a
/// Tokio timeout cannot preempt blocking code. This port deliberately has no authoritative/decommissioning result kind. An absent endpoint
/// or an empty/error result says nothing about a configured authority's causal-GC obligation.
pub trait EndpointDiscovery<A>: Send + Sync + 'static {
    /// Resolve a bounded snapshot of substrate addresses that may be worth probing.
    fn discover(&self, limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, A>;
}

/// Bounds on retained hints, probe work and resolver latency.
/// Defaults retain up to 256 candidates, emit up to 16 probes per second and allow a 5-second lookup.
#[derive(Clone, Copy, Debug)]
pub struct EndpointDiscoveryConfig {
    max_candidates: NonZeroUsize,
    max_probes: NonZeroUsize,
    timeout: Duration,
    probe_interval: Duration,
}

impl EndpointDiscoveryConfig {
    /// Configure a bounded discovery schedule.
    ///
    /// # Errors
    /// Rejects a probe budget larger than the candidate cap, or a zero timeout/interval.
    pub fn new(
        max_candidates: NonZeroUsize,
        max_probes: NonZeroUsize,
        timeout: Duration,
        probe_interval: Duration,
    ) -> io::Result<Self> {
        if max_probes > max_candidates || timeout.is_zero() || probe_interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid endpoint discovery limits",
            ));
        }
        Ok(Self {
            max_candidates,
            max_probes,
            timeout,
            probe_interval,
        })
    }
}

impl Default for EndpointDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_candidates: NonZeroUsize::new(256).unwrap(),
            max_probes: NonZeroUsize::new(16).unwrap(),
            timeout: Duration::from_secs(5),
            probe_interval: Duration::from_secs(1),
        }
    }
}

/// Bounded endpoint hints and a rate-limited, rotating probe schedule.
///
/// Resolve with `refresh` separately from driving a signed replica: a slow resolver must not
/// hold the replica's mutable borrow or delay its receive/reconciliation loop. Pass this state
/// to `SignedReplica::probe_discovered` (in the reconcile crate), then deliver its datagrams.
/// The caller owns refresh cadence and source selection. This object owns no authenticated
/// routes, allowlists, causal membership, or ACKs; refreshing it never removes such state.
/// Limits bound retained endpoints and outbound probe count, not the size of an endpoint value
/// or a custom resolver's internal memory. Generic endpoints need equality, not IP or hashing.
pub struct EndpointDiscoveryState<A> {
    config: EndpointDiscoveryConfig,
    candidates: Vec<A>,
    cursor: usize,
    last_probe: Option<Instant>,
}

impl<A: Clone + Eq> EndpointDiscoveryState<A> {
    /// Start with no hints and no emitted probes.
    pub fn new(config: EndpointDiscoveryConfig) -> Self {
        Self {
            config,
            candidates: Vec::new(),
            cursor: 0,
            last_probe: None,
        }
    }

    /// Resolve and atomically replace the hint set, keeping the existing set on failure.
    ///
    /// Deduplication preserves first occurrence order. A still-present next candidate keeps its
    /// place in rotation across refreshes; every refresh preserves the probe cooldown. An empty
    /// successful result clears only hints, never authenticated routes or causal blockers.
    ///
    /// # Errors
    /// Resolver errors, timeout, or a result exceeding the requested raw candidate cap. The
    /// resolver's allocation is its own responsibility; oversized vectors are rejected, not
    /// silently truncated. Dropping this future before completion leaves the prior state intact.
    pub async fn refresh(&mut self, source: &dyn EndpointDiscovery<A>) -> io::Result<()>
    where
        A: 'static,
    {
        let raw = tokio::time::timeout(
            self.config.timeout,
            source.discover(self.config.max_candidates),
        )
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, err))?
        .map_err(io::Error::other)?;
        if raw.len() > self.config.max_candidates.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "endpoint discovery exceeded candidate cap",
            ));
        }
        let mut candidates = Vec::with_capacity(raw.len());
        for candidate in raw {
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
        let next = self.candidates.get(self.cursor);
        let cursor = next
            .and_then(|endpoint| candidates.iter().position(|item| item == endpoint))
            .unwrap_or(0);
        self.candidates = candidates;
        self.cursor = cursor;
        Ok(())
    }

    /// Number of retained, distinct endpoint hints.
    pub fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    /// Take at most the configured probe budget, once per configured interval.
    ///
    /// Returns no candidates during cooldown or when the hint set is empty. A batch contains
    /// each endpoint at most once and rotates across the retained set on subsequent eligible
    /// calls. Taking a nonempty batch consumes its budget even if subsequent sending fails.
    /// Going backwards in `now` cannot bypass cooldown. This method never confirms a route.
    pub fn next_probes(&mut self, now: Instant) -> Vec<A> {
        if self.candidates.is_empty()
            || self.last_probe.is_some_and(|last| {
                now.saturating_duration_since(last) < self.config.probe_interval
            })
        {
            return Vec::new();
        }
        let count = self.config.max_probes.get().min(self.candidates.len());
        let mut probes = Vec::with_capacity(count);
        for _ in 0..count {
            probes.push(self.candidates[self.cursor].clone());
            self.cursor = (self.cursor + 1) % self.candidates.len();
        }
        self.last_probe = Some(now);
        probes
    }
}

#[cfg(test)]
mod tests;
