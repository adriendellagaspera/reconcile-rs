// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `Backpressure`, `WriteRejected`, and `ReplicatedMap::try_insert` (#83) — split out of
//! `write.rs` to keep it under the file-size budget (AGENTS.md §3); `try_update`'s counterpart
//! lives in `mutate.rs` next to the `update`/`upsert` core it shares (`mutate_live_checked`).
//! `WriteRejected` also wraps `value_size::ValueTooLarge` (#82): both `try_insert` and
//! `try_update` reject on either cause before touching the map.

use std::fmt;
use std::hash::Hash;

use crate::bounds::{Key, Value};
use crate::entry::Entry;

use super::value_size::{check_value_size, ValueTooLarge};
use super::ReplicatedMap;

/// Why [`try_insert`](ReplicatedMap::try_insert)/[`try_update`](ReplicatedMap::try_update)
/// rejected a write — either cause leaves the write **not applied**: map and broadcast are
/// all-or-nothing for these two calls (see [`Backpressure`]'s docs), the same guarantee whichever
/// reason triggers it.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum WriteRejected {
    /// The value's encoded size exceeds
    /// [`Config::max_value_size`](super::Config::max_value_size) (#82).
    TooLarge(ValueTooLarge),
    /// The write-broadcast egress budget
    /// ([`Config::max_concurrent_broadcasts`](super::Config::max_concurrent_broadcasts)) is
    /// exhausted (#83).
    Backpressure(Backpressure),
}

impl fmt::Display for WriteRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteRejected::TooLarge(err) => write!(f, "{err}"),
            WriteRejected::Backpressure(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for WriteRejected {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WriteRejected::TooLarge(err) => Some(err),
            WriteRejected::Backpressure(err) => Some(err),
        }
    }
}

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
    /// Fallible counterpart of [`insert`](Self::insert) (#83): checks `value`'s encoded size
    /// against [`Config::max_value_size`](super::Config::max_value_size) (#82) before touching
    /// anything, then claims a [`max_concurrent_broadcasts`](super::Config::max_concurrent_broadcasts)
    /// egress slot **before** touching the map, so a call either fully applies — locally and
    /// broadcast — or not at all. Always sends immediately, bypassing
    /// [`coalesce_window`](super::Config::coalesce_window) batching: a caller reaching for this
    /// feedback wants to know now.
    ///
    /// # Errors
    ///
    /// [`WriteRejected::TooLarge`] when `value`'s encoded size exceeds `max_value_size`, or
    /// [`WriteRejected::Backpressure`] when the egress budget is already at capacity. The map is
    /// untouched either way.
    ///
    /// # Panics
    ///
    /// See [`insert`](Self::insert) — the broadcast requires an ambient Tokio runtime, on the
    /// success path only.
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
    pub fn try_insert(&self, key: K, value: V) -> Result<Option<V>, WriteRejected> {
        check_value_size(&value, self.engine.max_value_size()).map_err(WriteRejected::TooLarge)?;
        match self
            .engine
            .try_insert(key, Entry::present(self.engine.clock_now(), value))
        {
            Ok(ret) => Ok(ret.and_then(|t| t.state.into())),
            Err(_) => Err(WriteRejected::Backpressure(Backpressure {
                in_flight: self.engine.broadcasts_in_flight(),
                max_in_flight: self.engine.max_concurrent_broadcasts(),
            })),
        }
    }
}
