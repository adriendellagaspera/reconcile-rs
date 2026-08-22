// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A seeded network-emulation decorator over the [`Transport`] port: injected one-way delay,
//! jitter, loss and reordering, configurable per directed link.
//!
//! `netem`-feature-gated dev/bench tooling, not part of the crate's default build: this
//! workspace's own `benches/`/`tests/netem.rs` enable it as a dev-dependency feature, and an
//! external consumer that needs the same instrument for its own reconciliation-cost measurements
//! depends on it the same way.
//!
//! Every other benchmark in this repository runs at RTT ≈ 0, which prices the axis RBSR is good at
//! (bytes) and zeroes the axis it is worst at (`SOTA.md` §1.3: sequential round-trips). This module
//! is the missing instrument, not a fix: it makes the round-trip column cost something.
//!
//! Split across two files by concern: this one owns the impairment *parameters* a caller
//! configures (`Probability`, `Rtt`, `Seed`, `Link`, `Netem`); [`transport`] owns the `Transport`
//! decorator itself and its delivery pump. Both halves re-export flat under `gossip::netem::*`.
//!
//! # Why not `turmoil`
//!
//! Whether `turmoil` fits was the natural question before building this. It is the right tool for
//! a different job:
//!
//! | `turmoil` ([tokio-rs/turmoil]) | what that costs here |
//! |---|---|
//! | time is simulated and advanced a `Builder::tick_duration` at a time (`simulation_duration` is "in simulated time") | Criterion reports wall-clock; a benchmark run inside the simulator would report the simulator's own tick arithmetic — the injected constant, read back |
//! | hosts are futures registered on a `Sim` and driven by `sim.run()` to a fixed simulated duration | Criterion owns the iteration loop, so `iter_custom` has no way to hand its samples to `sim.run()` |
//! | "runs multiple concurrent hosts within a single thread" | `gossip_propagation` deliberately measures N real per-node loops contending on one runtime (`benches/README.md`) |
//! | networking is `turmoil::net`, a drop-in replacement for `tokio::net` | `gossip::UdpTransport` wraps `tokio::net::UdpSocket`, so a `turmoil` lane needs its own [`Transport`] impl **as well as** the simulator — this decorator plus more, not instead of it |
//!
//! So: bespoke, and no new dependency at all ([`rand`] is already a dependency). The knobs
//! below deliberately mirror `turmoil`'s (`min_message_latency`/`max_message_latency`/`fail_rate`),
//! so swapping a `turmoil` lane in later is a substitution rather than a rewrite.
//!
//! # Model
//!
//! Impairment is applied **send-side**, keyed by destination: a datagram is drawn against its
//! link's model, then either dropped or queued for delivery at `now + delay`. A per-node pump task
//! delivers due datagrams in due order to the wrapped transport. Consequences worth knowing:
//!
//! - `send_to` returns immediately, as UDP does — the delay is propagation, not back-pressure.
//! - a drop is `Ok(n)`, not an error: a lost datagram is indistinguishable from a delivered one at
//!   the sender, which is the property the protocol has to survive.
//! - jitter reorders on its own; [`Link::with_reorder`] adds an explicit displacement on top.
//! - delivery order is by `(due, send order)`, so a run is reproducible down to the scheduler.
//!
//! # Determinism
//!
//! Each directed link gets its own PRNG stream, seeded from [`Seed`] mixed with the two endpoint
//! addresses, and every datagram consumes exactly three draws (loss, jitter, reorder) whatever the
//! outcome. So the impairment sequence *per link* replays exactly, given the same datagram
//! sequence on it. What is **not** reproducible bit-for-bit is the interleaving of concurrent tasks
//! on a multi-threaded runtime — record the seed (the benchmarks print it) and read the results as
//! reproducible to within the usual benchmark noise.
//!
//! [tokio-rs/turmoil]: https://github.com/tokio-rs/turmoil

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::Transport;

mod transport;

pub use transport::{Impairments, NetemTransport};

/// A probability in `[0, 1]`.
///
/// Saturating rather than fallible, as `rbsr::FanOut::new` is: an out-of-range or NaN input is
/// clamped at construction, so an invalid instance cannot exist and no call site has to check.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Probability(f64);

impl Probability {
    /// Never.
    pub const ZERO: Probability = Probability(0.0);

    /// Always.
    pub const ALWAYS: Probability = Probability(1.0);

    /// A percentage — `Probability::percent(0.1)` is one datagram in a thousand. Percent rather
    /// than a bare fraction because that is the unit loss is quoted in.
    pub fn percent(percent: f64) -> Probability {
        // `f64::max` returns the non-NaN operand, so this also maps NaN to zero.
        Probability((percent.max(0.0) / 100.0).min(1.0))
    }

    /// The probability as a fraction of one.
    pub fn as_fraction(self) -> f64 {
        self.0
    }

    /// The Criterion benchmark-id parameter: `loss=0.1%`, `loss=1%`. Tenths of a percent in
    /// integer arithmetic, for the same reason as [`Rtt::label`].
    pub fn label(self) -> String {
        let tenths = (self.0 * 1_000.0).round() as u64;
        if tenths % 10 == 0 {
            format!("loss={}%", tenths / 10)
        } else {
            format!("loss={}.{}%", tenths / 10, tenths % 10)
        }
    }

    /// Draw once. Always consumes exactly one value from `rng`, so a link's stream position does
    /// not depend on its outcomes.
    fn hits(self, rng: &mut StdRng) -> bool {
        rng.gen_bool(self.0)
    }
}

/// A round-trip time.
///
/// The decorator injects **one-way** delay in each direction, and a sweep is stated in RTT because
/// that is what an operator measures. Owning the halving on the type is the whole point of it:
/// confusing the two is a silent factor of two in every number this instrument produces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rtt(Duration);

impl Rtt {
    /// The loopback lane: what every other benchmark in this repository runs at.
    pub const ZERO: Rtt = Rtt(Duration::ZERO);

    /// A round-trip time in milliseconds. Fractional, so the sweep can reach 0.1 ms; negative and
    /// NaN saturate to zero.
    pub fn from_millis(millis: f64) -> Rtt {
        Rtt(Duration::from_secs_f64(millis.max(0.0) / 1_000.0))
    }

    /// The one-way propagation delay injected in each direction — half the round trip.
    pub fn one_way(self) -> Duration {
        self.0 / 2
    }

    /// The Criterion benchmark-id parameter: `0ms`, `0.1ms`, `50ms`. Integer arithmetic on
    /// microseconds, so the label is identical on every machine (a float format is not).
    pub fn label(self) -> String {
        let micros = self.0.as_micros();
        let (millis, tenths) = (micros / 1_000, (micros % 1_000) / 100);
        if tenths == 0 {
            format!("rtt={millis}ms")
        } else {
            format!("rtt={millis}.{tenths}ms")
        }
    }
}

/// The seed of an emulation run. Record it with the results: it is what makes the losses replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seed(u64);

impl Seed {
    /// The seed the benchmarks run at, absent a reason to vary it.
    pub const DEFAULT: Seed = Seed(0x5eed_0280);

    pub fn new(seed: u64) -> Seed {
        Seed(seed)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// The impairment applied to one directed link.
///
/// Directed: `a → b` and `b → a` are configured separately, which is what makes an asymmetric or
/// geographic topology expressible (`Config::remote_interval`/`remote_fanout` model the same split
/// on the protocol side).
#[derive(Clone, Copy, Debug)]
pub struct Link {
    delay: Duration,
    jitter: Duration,
    loss: Probability,
    reorder: Probability,
}

impl Link {
    /// A perfect link: no delay, no loss. The RTT-≈-0 lane, i.e. what every other benchmark in
    /// this repository measures — still routed through the pump, so it prices the harness itself
    /// and every other lane's delta is against it rather than against a different code path.
    pub const PERFECT: Link = Link {
        delay: Duration::ZERO,
        jitter: Duration::ZERO,
        loss: Probability::ZERO,
        reorder: Probability::ZERO,
    };

    /// A link whose one-way delay is half of `rtt`.
    pub fn at(rtt: Rtt) -> Link {
        Link {
            delay: rtt.one_way(),
            ..Link::PERFECT
        }
    }

    /// Uniform swing of ±`jitter` around the one-way delay, clamped at zero. Jitter reorders on its
    /// own: a datagram drawn short overtakes one drawn long.
    pub fn with_jitter(mut self, jitter: Duration) -> Link {
        self.jitter = jitter;
        self
    }

    /// Per-datagram drop probability.
    pub fn with_loss(mut self, loss: Probability) -> Link {
        self.loss = loss;
        self
    }

    /// Per-datagram probability of an extra one-way delay's displacement, which lands the datagram
    /// behind those sent after it. Relative to the link's own propagation time, so a zero-delay
    /// link cannot be reordered — on such a link there is no flight to be overtaken in.
    pub fn with_reorder(mut self, reorder: Probability) -> Link {
        self.reorder = reorder;
        self
    }

    /// Draw this datagram's fate. Exactly three draws, whatever the outcome, so a link's stream
    /// position is a function of how many datagrams crossed it and nothing else.
    fn draw(self, rng: &mut StdRng) -> Option<Duration> {
        let lost = self.loss.hits(rng);
        let swing: f64 = rng.gen_range(-1.0..=1.0);
        let reordered = self.reorder.hits(rng);
        if lost {
            return None;
        }
        let offset = self.jitter.mul_f64(swing.abs());
        let jittered = if swing < 0.0 {
            self.delay.saturating_sub(offset)
        } else {
            self.delay + offset
        };
        Some(if reordered {
            jittered + self.delay
        } else {
            jittered
        })
    }
}

/// One node's view of the network: a default link to every peer, plus per-destination overrides.
#[derive(Clone, Debug)]
pub struct Netem {
    default_link: Link,
    per_destination: HashMap<IpAddr, Link>,
    seed: Seed,
}

impl Netem {
    /// The same `link` to every destination.
    pub fn uniform(link: Link, seed: Seed) -> Netem {
        Netem {
            default_link: link,
            per_destination: HashMap::new(),
            seed,
        }
    }

    /// Override the link to one destination — the seam an asymmetric or geographic topology is
    /// built from (a node with two far peers and six near ones is six calls).
    pub fn with_link_to(mut self, destination: IpAddr, link: Link) -> Netem {
        self.per_destination.insert(destination, link);
        self
    }

    fn link_to(&self, destination: &SocketAddr) -> Link {
        self.per_destination
            .get(&destination.ip())
            .copied()
            .unwrap_or(self.default_link)
    }
}

/// Mix a seed and both endpoints into a distinct PRNG stream per directed link.
///
/// Explicit SplitMix64 rather than `DefaultHasher`, whose output is documented as unstable across
/// Rust releases — these benchmarks are supposed to be reproducible across machines and over time
/// (`benches/README.md`), and a seed that only holds within one toolchain is not a seed.
fn stream_seed(seed: Seed, source: SocketAddr, destination: SocketAddr) -> u64 {
    fn mix(state: &mut u64, value: u64) {
        *state = state
            .wrapping_add(value)
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        *state = z ^ (z >> 31);
    }
    fn endpoint(state: &mut u64, addr: SocketAddr) {
        match addr.ip() {
            IpAddr::V4(v4) => mix(state, u32::from(v4) as u64),
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                mix(state, (bits >> 64) as u64);
                mix(state, bits as u64);
            }
        }
        mix(state, addr.port() as u64);
    }
    let mut state = seed.get();
    endpoint(&mut state, source);
    endpoint(&mut state, destination);
    state
}
