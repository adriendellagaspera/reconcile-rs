// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Shared benchmark utilities.
//!
//! [`protocol_cost`] measures reconciliation cost, while [`transport_model`] and the experiment
//! helpers support repository-only benchmark analysis. This crate is not published.
#![forbid(unsafe_code)]

pub mod corpus;
pub mod experiment;
pub mod protocol_cost;
pub mod transport_model;
