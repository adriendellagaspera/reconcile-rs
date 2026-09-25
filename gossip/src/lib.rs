// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Network adapters used by `reconcile`.
//!
//! This crate owns datagram transport, wire encoding, authentication, replay protection, peer
//! discovery, and optional network emulation. It does not depend on the replicated-value domain:
//! payloads are bytes and peers are addresses.
//!
//! Applications should normally depend on
//! [`reconcile`](https://crates.io/crates/reconcile), which re-exports the supported API.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod auth;
// Named after the external crate it wraps; `::bincode::…` disambiguates from this module.
pub mod bincode;
pub mod discovery;
pub mod gen_ip;
#[cfg(feature = "netem")]
pub mod netem;
pub mod replay;
pub mod transport;

pub use discovery::{
    DiscoverFuture, Discovery, DiscoveryError, DiscoveryKind, DnsDiscovery, DnsDiscoveryError,
    RandomProbe,
};
pub use transport::{InMemoryNetwork, InMemoryTransport, Transport, UdpTransport};

// Re-export dependencies whose types appear in public signatures.
pub use async_trait::async_trait;
pub use ipnet;
pub use parking_lot;
pub use rand;
pub use tokio;
