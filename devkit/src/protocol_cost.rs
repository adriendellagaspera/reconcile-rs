// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! What one full RBSR reconciliation costs, counted rather than timed: messages, advertised
//! ranges, refinement bytes, datagrams/fragments, IDLIST outcomes and local RSOS queries.
//! Drives `rsos`/`rbsr` directly — no dependency on any wire format, so a caller prices whatever
//! payload shape it ships by passing [`reconcile`] a pricing closure, rather than this crate
//! assuming one.

use std::cell::Cell;
use std::ops::{Add, RangeBounds};

use bincode::Options;
use rand::rngs::StdRng;
use rbsr::{
    initial_ranges, protocol_round_with_policy, EnumerationRange, RangeAggregate, RefinementPolicy,
};
use rsos::{Aggregate, Rsos};

use crate::transport_trace::{
    Direction, MessageKind, ProtocolTrace, TraceMessage, TraceStage,
};

/// The payload one datagram can carry: the IPv4 ceiling, the most optimistic split point — a
/// keyed deployment subtracts the authenticator's overhead.
pub const MAX_DATAGRAM_PAYLOAD: usize = 65_507;

/// The payload one IP fragment carries on a 1500-byte-MTU path. Approximate on purpose: losing
/// any fragment loses the datagram, so only the order of magnitude matters.
pub const MTU_FRAGMENT_PAYLOAD: usize = 1_472;

/// The RSOS queries a reconciliation performed — the paper's local-cost model `T_loc` in counts
/// rather than seconds, and the half of a policy's cost that never appears on the wire.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Queries {
    pub aggregate: usize,
    pub rank: usize,
    pub select: usize,
}

impl Add for Queries {
    type Output = Queries;

    fn add(self, other: Queries) -> Queries {
        Queries {
            aggregate: self.aggregate + other.aggregate,
            rank: self.rank + other.rank,
            select: self.select + other.select,
        }
    }
}

/// A read-only RSOS tallying the three query kinds [`reconcile`] performs.
/// Implements `rsos:Rsos`, not `rbsr:RsosView`: the blanket impl makes a second `RsosView` impl
/// a coherence conflict.
pub struct Counting<'a, S> {
    inner: &'a S,
    aggregate: Cell<usize>,
    rank: Cell<usize>,
    select: Cell<usize>,
}

impl<'a, S> Counting<'a, S> {
    pub fn new(inner: &'a S) -> Counting<'a, S> {
        Counting {
            inner,
            aggregate: Cell::new(0),
            rank: Cell::new(0),
            select: Cell::new(0),
        }
    }

    pub fn queries(&self) -> Queries {
        Queries {
            aggregate: self.aggregate.get(),
            rank: self.rank.get(),
            select: self.select.get(),
        }
    }
}

impl<K, S: Rsos<K>> Rsos<K> for Counting<'_, S> {
    type Value = S::Value;

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn aggregate<R: RangeBounds<K>>(&self, range: R) -> Aggregate {
        self.aggregate.set(self.aggregate.get() + 1);
        self.inner.aggregate(range)
    }

    fn rank(&self, z: &K) -> usize {
        self.rank.set(self.rank.get() + 1);
        self.inner.rank(z)
    }

    fn select(&self, r: usize) -> &K {
        self.select.set(self.select.get() + 1);
        self.inner.select(r)
    }

    fn enumerate<'b, R: RangeBounds<K> + 'b>(
        &'b self,
        range: R,
    ) -> impl Iterator<Item = (&'b K, &'b Self::Value)> + 'b
    where
        K: Ord + 'b,
        Self::Value: 'b,
    {
        self.inner.enumerate(range)
    }

    fn insert(&mut self, _key: K, _value: Self::Value) -> Option<Self::Value> {
        // The reconciliation driver reads and never writes (`RsosView` does not even name these
        // two). They exist only because `Rsos` is the seven-operation contract; wrapping a `&S`
        // could not honour them anyway.
        unreachable!("the reconciliation driver never mutates the store")
    }

    fn delete(&mut self, _key: &K) -> Option<Self::Value> {
        unreachable!("the reconciliation driver never mutates the store")
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RoundCost {
    pub direction: Direction,
    pub ranges: usize,
    pub refinement_bytes: usize,
    pub enumerated_elements: usize,
    pub enumerated_bytes: Vec<usize>,
}

/// What one full reconciliation cost.
#[derive(Debug, Default)]
pub struct Cost {
    /// One-way protocol messages, i.e. how many times a batch of active ranges crossed the wire.
    /// Halve it for a round-trip count.
    pub messages: usize,
    /// Total `RangeAggregate`s advertised across every message.
    pub ranges: usize,
    /// Total bincode-encoded bytes of those aggregates. The refinement half of
    /// `Cost::total_bytes`; it does not move with the enumerated payload.
    pub refinement_bytes: usize,
    /// Datagrams the refinement batches become, at [`MAX_DATAGRAM_PAYLOAD`] per datagram.
    pub datagrams: usize,
    /// IP fragments those datagrams become, at [`MTU_FRAGMENT_PAYLOAD`] per fragment.
    pub fragments: usize,
    /// The largest single refinement message, in ranges, and the bytes those ranges encode to.
    pub largest_message: usize,
    pub largest_message_bytes: usize,
    /// Ranges handed back for explicit enumeration (the paper's IDLIST outcome), and the elements
    /// those ranges actually contain.
    pub enumerations: usize,
    pub enumerated_elements: usize,
    /// What those elements cost on the wire, one entry per payload variant `price_element`
    /// returned — the value half of [`Cost::total_bytes`]. Empty when `reconcile` was
    /// called with `price_element: None`.
    pub enumerated_bytes: Vec<usize>,
    /// Local RSOS queries, summed over both peers.
    pub queries: Queries,
    /// Per-driver-round detail used to reconstruct dependency-preserving transport traces.
    pub rounds_trace: Vec<RoundCost>,
}

impl Cost {
    /// Everything this reconciliation put on the wire, one entry per payload variant priced: the
    /// refinement traffic plus the values the IDLIST outcomes ship.
    pub fn total_bytes(&self) -> Vec<usize> {
        self.enumerated_bytes
            .iter()
            .map(|&bytes| bytes + self.refinement_bytes)
            .collect()
    }

    /// Reconstruct the logical one-way wire sequence for one priced payload variant.
    ///
    /// Round `i`'s enumeration payload is produced while processing its incoming refinement and
    /// is therefore coalesced with round `i+1`'s opposite-direction refinement when one exists.
    /// A final enumeration-only response becomes its own terminal stage.
    pub fn transport_trace(&self, payload_variant: usize) -> ProtocolTrace {
        let mut stages = Vec::new();
        if let Some(first) = self.rounds_trace.first() {
            stages.push(TraceStage {
                messages: vec![TraceMessage {
                    direction: first.direction,
                    kind: MessageKind::Refinement,
                    payload_bytes: first.refinement_bytes,
                    streamable: false,
                }],
            });
        }

        for index in 1..self.rounds_trace.len() {
            let previous = &self.rounds_trace[index - 1];
            let current = &self.rounds_trace[index];
            let enum_bytes = previous
                .enumerated_bytes
                .get(payload_variant)
                .copied()
                .unwrap_or(0);
            let mut messages = Vec::new();
            if current.refinement_bytes > 0 {
                messages.push(TraceMessage {
                    direction: current.direction,
                    kind: MessageKind::Refinement,
                    payload_bytes: current.refinement_bytes,
                    streamable: false,
                });
            }
            if enum_bytes > 0 {
                messages.push(TraceMessage {
                    direction: current.direction,
                    kind: MessageKind::Enumeration,
                    payload_bytes: enum_bytes,
                    streamable: false,
                });
            }
            if !messages.is_empty() {
                stages.push(TraceStage { messages });
            }
        }

        if let Some(last) = self.rounds_trace.last() {
            let enum_bytes = last
                .enumerated_bytes
                .get(payload_variant)
                .copied()
                .unwrap_or(0);
            if enum_bytes > 0 {
                stages.push(TraceStage {
                    messages: vec![TraceMessage {
                        direction: last.direction.reverse(),
                        kind: MessageKind::Enumeration,
                        payload_bytes: enum_bytes,
                        streamable: false,
                    }],
                });
            }
        }

        ProtocolTrace {
            protocol: "rbsr".to_owned(),
            stages,
        }
    }

    /// What this reconciliation *decided*, as opposed to what those decisions encoded to.
    pub fn decisions(&self) -> Decisions {
        Decisions {
            messages: self.messages,
            ranges: self.ranges,
            enumerations: self.enumerations,
            enumerated_elements: self.enumerated_elements,
            queries: self.queries,
        }
    }
}

/// The payload-independent half of a [`Cost`]: every outcome the driver reached, none of the bytes
/// they encoded to — `datagrams`/`fragments` sit on the byte side, being ceilings over
/// `Cost::refinement_bytes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Decisions {
    pub messages: usize,
    pub ranges: usize,
    pub enumerations: usize,
    pub enumerated_elements: usize,
    pub queries: Queries,
}

/// Drive both peers to convergence under `policy`, counting what crosses the wire. Every
/// mismatching range is resolved or strictly refined, so the loop terminates; the guard is a bug
/// net.
/// Both peers run the same policy — they need not, but a mixed pair would measure neither.
/// `price_element`, when `Some`, is called once per enumerated element (its key) and must return
/// the wire-encoded byte length(s) of whatever the caller's own transport would ship for it — one
/// entry per payload variant to price side by side. `None` counts enumerated elements without
/// pricing them, for a timed drive where encoding a real payload would put the caller's own
/// encoder inside the measurement.
/// `rng` supplies the session-scoped randomized split offset and is reused across all rounds.
pub fn reconcile<S: Rsos<u64>>(
    a: &S,
    b: &S,
    policy: &dyn RefinementPolicy,
    mut price_element: Option<&mut dyn FnMut(u64) -> Vec<usize>>,
    rng: &mut StdRng,
) -> Cost {
    let mut cost = Cost::default();
    let mut active: Vec<RangeAggregate<u64>> = initial_ranges(a);
    // `initial_ranges` came from `a`, so `b` answers first, and the responder alternates from there.
    let mut responder_is_b = true;

    while !active.is_empty() {
        // `DefaultOptions` matches `gossip:bincode:encode`'s own encoder config (little-endian,
        // varint integers) — only the length is needed here, never the bytes themselves.
        let round_bytes: usize = active
            .iter()
            .map(|segment| {
                bincode::DefaultOptions::new()
                    .serialized_size(segment)
                    .expect("computing a RangeAggregate's encoded length cannot fail")
                    as usize
            })
            .sum();
        let direction = if responder_is_b {
            Direction::AToB
        } else {
            Direction::BToA
        };
        let mut round = RoundCost {
            direction,
            ranges: active.len(),
            refinement_bytes: round_bytes,
            ..RoundCost::default()
        };
        cost.messages += 1;
        cost.ranges += active.len();
        cost.refinement_bytes += round_bytes;
        cost.datagrams += round_bytes.div_ceil(MAX_DATAGRAM_PAYLOAD).max(1);
        cost.fragments += round_bytes.div_ceil(MTU_FRAGMENT_PAYLOAD).max(1);
        update_largest_message(
            &mut cost.largest_message,
            &mut cost.largest_message_bytes,
            active.len(),
            round_bytes,
        );

        let mut children = Vec::new();
        let mut enumerations: Vec<EnumerationRange<u64>> = Vec::new();
        let responder = if responder_is_b { b } else { a };
        protocol_round_with_policy(
            responder,
            policy,
            active,
            &mut children,
            &mut enumerations,
            rng,
        );
        cost.enumerations += enumerations.len();
        // What an IDLIST actually ships. `Enumerate(l, u)` is the paper's own operation, so this is
        // a real cost of the policy, not an artifact of how the caller drives it.
        for range in enumerations {
            for (&key, _) in responder.enumerate(range) {
                cost.enumerated_elements += 1;
                round.enumerated_elements += 1;
                if let Some(price) = price_element.as_deref_mut() {
                    let bytes = price(key);
                    if needs_enumerated_bytes_init(&cost.enumerated_bytes, &bytes) {
                        cost.enumerated_bytes = vec![0; bytes.len()];
                    }
                    if needs_enumerated_bytes_init(&round.enumerated_bytes, &bytes) {
                        round.enumerated_bytes = vec![0; bytes.len()];
                    }
                    for (total, element) in cost.enumerated_bytes.iter_mut().zip(&bytes) {
                        *total += *element;
                    }
                    for (total, element) in round.enumerated_bytes.iter_mut().zip(bytes) {
                        *total += element;
                    }
                }
            }
        }

        cost.rounds_trace.push(round);
        active = children;
        responder_is_b = !responder_is_b;
        assert!(
            cost.messages < 100_000,
            "reconciliation failed to converge — the refinement is not shrinking"
        );
    }
    cost
}

/// Track the largest single round seen so far, in ranges and the bytes those ranges encoded to. A
/// strictly larger round replaces the previous largest; a tie leaves it (and its bytes) alone.
fn update_largest_message(
    largest_message: &mut usize,
    largest_message_bytes: &mut usize,
    len: usize,
    bytes: usize,
) {
    if len > *largest_message {
        *largest_message = len;
        *largest_message_bytes = bytes;
    }
}

/// Whether `enumerated_bytes` needs sizing for its first payload: true only when it is still empty
/// *and* this call actually priced something. An empty `bytes` from a later, differently-shaped
/// call must never re-trigger this and wipe out totals already accumulated.
fn needs_enumerated_bytes_init(enumerated_bytes: &[usize], bytes: &[usize]) -> bool {
    enumerated_bytes.is_empty() && !bytes.is_empty()
}

#[cfg(test)]
mod tests;
