// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{fmt, io};

use super::ConfigError;

/// Why constructing a replicated store or read replica failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConstructionError {
    /// The supplied [`Config`](super::Config) is invalid.
    Config(ConfigError),
    /// Binding or querying the transport failed.
    Io(io::Error),
}

impl fmt::Display for ConstructionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Io(error) => Some(error),
        }
    }
}

impl From<ConfigError> for ConstructionError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<io::Error> for ConstructionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
