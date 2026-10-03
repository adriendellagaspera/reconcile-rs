// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashSet;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::trace;

use crate::bounds::{Key, Value};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::transport::Transport;
use super::{send_messages_paced, Message, Replica, SendPorts};

/// Which channel a paced bulk dump resolves ranges against: a `differences` batch that
/// loses the per-peer dump-slot race is stashed by channel, since the dated and value-only
/// channels share one slot but resolve ranges against different trees and message variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DumpChannel {
    /// `self.map`, resolves to [`Message::EntryUpdate`].
    Dated,
    /// `self.projection`, resolves to [`Message::StateUpdate`].
    ValueOnly,
}

impl<K: Key + Hash, V: Value> Replica<K, V> {
    /// Bundle this engine's outbound ports and send state for the batched-message helpers
    /// ([`send_messages_to`] / [`send_messages_paced`]). See [`SendPorts`].
    pub(super) fn send_ports(&self) -> SendPorts<'_, dyn Transport> {
        SendPorts {
            transport: &*self.transport,
            authenticator: &self.authenticator,
            sender_counter: &self.sender_counter,
            framing: self.framing,
        }
    }

    /// Claim both a per-peer in-flight slot and a global dump slot, or `None` if either is taken.
    ///
    /// Called **before** snapshotting the range, so a skipped dump allocates nothing; the guards
    /// release on drop, panic included.
    pub(super) fn try_claim_dump_slot(
        &self,
        peer: SocketAddr,
    ) -> Option<(BulkInFlightGuard, BulkDumpCountGuard)> {
        // Per-peer guard: at most one dump per peer at a time.
        if !self.bulk_in_flight.write().insert(peer) {
            return None;
        }
        // Global budget: at most `max_concurrent_bulk_dumps` across all peers. The
        // compare-exchange loop increments only if currently below the cap. If at cap, release the
        // per-peer mark before returning so that slot is not leaked.
        let budget = self.max_concurrent_bulk_dumps;
        let claimed = self
            .bulk_dumps_in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n < budget {
                    Some(n + 1)
                } else {
                    None
                }
            })
            .is_ok();
        if !claimed {
            self.bulk_in_flight.write().remove(&peer);
            trace!("skipped bulk dump to {peer}: global dump budget ({budget}) exhausted");
            return None;
        }
        Some((
            BulkInFlightGuard {
                set: Arc::clone(&self.bulk_in_flight),
                peer,
            },
            BulkDumpCountGuard {
                counter: Arc::clone(&self.bulk_dumps_in_flight),
            },
        ))
    }

    /// Send a bulk batch of differing values to one peer on a detached, **rate-paced** task —
    /// the cold-sync path.
    ///
    /// Three mechanisms bound it, all against the same amplification: pacing to
    /// [`bulk_send_rate`](Inner::bulk_send_rate) off the receive loop, one dump per peer (an
    /// `Update` triggers no reply, so the holder's reconcile timer would otherwise re-dump ranges
    /// in transit), and a global [`try_claim_dump_slot`](Self::try_claim_dump_slot) budget
    /// bounding total in-flight snapshot memory.
    ///
    /// Before releasing `peer`'s slot, drains [`stash_pending_dump`](Self::stash_pending_dump)'s
    /// stash for `channel`/`peer` and sends that too, looping until nothing more is pending
    /// before releasing the slot: a `differences` batch discovered while this task was already sending must not wait
    /// for a fresh round to be noticed.
    pub(super) fn spawn_paced_send(
        &self,
        messages: Vec<Message<K, Entry<Timestamp, V>, State<V>>>,
        peer: SocketAddr,
        peer_guard: BulkInFlightGuard,
        global_guard: BulkDumpCountGuard,
        channel: DumpChannel,
    ) {
        let transport = Arc::clone(&self.transport);
        let authenticator = self.authenticator.clone();
        let sender_counter = Arc::clone(&self.sender_counter);
        let rate = self.bulk_send_rate;
        let framing = self.framing;
        let map = Arc::clone(&self.map);
        let projection = Arc::clone(&self.projection);
        let pending_dumps = Arc::clone(&self.pending_dumps);
        let pending_value_dumps = Arc::clone(&self.pending_value_dumps);
        tokio::spawn(async move {
            // Hold both RAII guards for the lifetime of this task, releasing them only once
            // nothing more is pending for `peer` on `channel` — even if aborted or panicking.
            let _peer_guard = peer_guard;
            let _global_guard = global_guard;
            let ports = SendPorts {
                transport: &*transport,
                authenticator: &authenticator,
                sender_counter: &sender_counter,
                framing,
            };
            let mut send_buf = Vec::new();
            let mut messages = messages;
            loop {
                send_messages_paced(&messages, &ports, &peer, &mut send_buf, rate).await;
                let stash = match channel {
                    DumpChannel::Dated => &pending_dumps,
                    DumpChannel::ValueOnly => &pending_value_dumps,
                };
                let Some(ranges) = stash.write().remove(&peer).filter(|r| !r.is_empty()) else {
                    break;
                };
                messages = Vec::new();
                match channel {
                    DumpChannel::Dated => {
                        let guard = map.load_full();
                        for range in ranges {
                            for (k, v) in guard.range(range) {
                                messages.push(Message::EntryUpdate((k.clone(), v.clone())));
                            }
                        }
                    }
                    DumpChannel::ValueOnly => {
                        let guard = projection.load_full();
                        for range in ranges {
                            for (k, v) in guard.range(range) {
                                messages.push(Message::StateUpdate((k.clone(), v.clone())));
                            }
                        }
                    }
                }
                if messages.is_empty() {
                    break;
                }
            }
        });
    }
}

/// RAII marker that a bulk dump to `peer` is in flight. Clearing on `Drop` means a panicking send
/// task cannot wedge a peer into permanently transferring.
pub(super) struct BulkInFlightGuard {
    set: Arc<RwLock<HashSet<SocketAddr>>>,
    peer: SocketAddr,
}

impl Drop for BulkInFlightGuard {
    fn drop(&mut self) {
        self.set.write().remove(&self.peer);
    }
}

/// RAII counter-decrement for the global concurrent-dump budget. Decrements the shared atomic on
/// `Drop`, guaranteeing the slot is freed even if the task holding it panics or is aborted. See
/// [`Replica::try_claim_dump_slot`].
pub(super) struct BulkDumpCountGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for BulkDumpCountGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Release);
    }
}
