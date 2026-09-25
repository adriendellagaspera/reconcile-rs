// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Last-write-wins register domain used by `reconcile`.
//!
//! This crate defines register entries, timestamps and clock arithmetic, key/value bounds, and the
//! persistence contract. It contains no network, async-runtime, wire-codec, or wall-clock adapter.
//!
//! Applications should normally depend on
//! [`reconcile`](https://crates.io/crates/reconcile), which re-exports the supported API.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod bounds;
pub mod clock;
pub mod entry;
pub mod persistence;

pub use bounds::{Key, Value};
pub use clock::{Clock, Timestamp};
pub use entry::{Entry, State};
pub use persistence::{DatedEntries, InMemoryPersistence, PersistedState, Persistence};
