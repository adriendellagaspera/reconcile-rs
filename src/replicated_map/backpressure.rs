// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `Backpressure` and `ReplicatedMap::try_insert` (#83) — split out of `write.rs` to keep it
//! under the file-size budget (AGENTS.md §3); `try_update`'s counterpart lives in `mutate.rs`
//! next to the `update`/`upsert` core it shares (`mutate_live_no_broadcast`).

use std::fmt;
use std::hash::Hash;

use crate::bounds::{Key, Value};
use crate::entry::Entry;

use super::ReplicatedMap;

/// Returned by [`try_insert`](ReplicatedMap::try_insert)/[`try_update`](ReplicatedMap::try_update)
/// when the write-broadcast egress budget
/// ([`Config::max_concurrent_broadcasts`](super::Config::max_concurrent_broadcasts), #83) is
/// exhausted.
///
/// The write is **not** applied when this is returned — map and broadcast are all-or-nothing for
/// these two calls, unlike their infallible counterparts
/// ([`insert`](ReplicatedMap::insert)/[`update`](ReplicatedMap::update)/
/// [`insert_bulk`](ReplicatedMap::insert_bulk)), which always apply the write and, at the same
/// budget, silently skip only that call's eager broadcast — recovered by the next periodic
/// reconciliation round or repair retry (#23), the same bounded cost an already-tolerated lost
/// datagram is. Prefer the infallible methods for ordinary writes; reach for `try_insert`/
/// `try_update` when the caller wants to know egress is falling behind and decide for itself
/// (retry, buffer, drop) rather than rely on that backstop.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub struct Backpressure {
    /// Write-broadcast tasks in flight at the moment the slot claim failed.
    pub in_flight: usize,
    /// The configured [`max_concurrent_broadcasts`](super::Config::max_concurrent_broadcasts)
    /// budget.
    pub max_in_flight: usize,
}

impl fmt::Display for Backpressure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "write-broadcast egress budget exhausted: {}/{} broadcasts in flight",
            self.in_flight, self.max_in_flight
        )
    }
}

impl std::error::Error for Backpressure {}

impl<K: Key + Hash, V: Value> ReplicatedMap<K, V> {
    /// Fallible counterpart of [`insert`](Self::insert) (#83): claims a
    /// [`max_concurrent_broadcasts`](super::Config::max_concurrent_broadcasts) egress slot
    /// **before** touching the map, so a call either fully applies — locally and broadcast — or
    /// not at all. Always sends immediately, bypassing [`coalesce_window`](super::Config::coalesce_window)
    /// batching: a caller reaching for backpressure feedback wants to know now.
    ///
    /// # Errors
    ///
    /// [`Backpressure`] when the egress budget is already at capacity. The map is untouched.
    ///
    /// # Panics
    ///
    /// See [`insert`](Self::insert) — the broadcast requires an ambient Tokio runtime.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// use reconcile::{replicated_map::Config, InMemoryNetwork, ReplicatedMap};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let network = InMemoryNetwork::new();
    /// let transport = Arc::new(network.bind("127.0.0.1:8309".parse().unwrap()));
    /// let store = ReplicatedMap::<String, i32>::new_with_transport(
    ///     Config::default().with_insecure_no_key(),
    ///     transport,
    /// );
    ///
    /// assert_eq!(store.try_insert("a".to_string(), 1), Ok(None));
    /// assert_eq!(store.try_insert("a".to_string(), 2), Ok(Some(1)));
    /// assert_eq!(store.get_cloned(&"a".to_string()), Some(2));
    /// # }
    /// ```
    pub fn try_insert(&self, key: K, value: V) -> Result<Option<V>, Backpressure> {
        match self
            .engine
            .try_insert(key, Entry::present(self.engine.clock_now(), value))
        {
            Ok(ret) => Ok(ret.and_then(|t| t.state.into())),
            Err(_) => Err(Backpressure {
                in_flight: self.engine.broadcasts_in_flight(),
                max_in_flight: self.engine.max_concurrent_broadcasts(),
            }),
        }
    }
}
