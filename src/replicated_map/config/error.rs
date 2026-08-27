// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;

use super::MAX_NETS;

/// Why a [`Config`](super::Config) operation was rejected.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The operation would exceed [`MAX_NETS`] declared networks.
    TooManyNets,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::TooManyNets => write!(f, "at most {MAX_NETS} networks are supported"),
        }
    }
}

impl std::error::Error for ConfigError {}
