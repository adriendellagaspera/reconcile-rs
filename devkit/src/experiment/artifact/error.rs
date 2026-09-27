use std::error::Error;
use std::fmt;

use crate::experiment::{Capability, ContractError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvalidExperimentReport {
    UnsupportedSchemaVersion(u32),
    EmptyArchitectures,
    EmptyObservations,
    EmptyPeerContext,
    InvalidContextMetric(&'static str),
    DuplicateArchitecture(String),
    DuplicateRequiredCapability(String, Capability),
    UnknownArchitecture(String),
    DuplicatePreparedState(String),
    ConflictingPreparedState(String),
    UnknownPreparedStateArchitecture(String),
    PreparedStateViewMismatch(String),
    DuplicateObservation(String, u32, u64),
    ConflictingArchitecture(String),
    IncompatibleJoin,
    EmptyJoin,
    Contract(ContractError),
}

impl fmt::Display for InvalidExperimentReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for InvalidExperimentReport {}

impl From<ContractError> for InvalidExperimentReport {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

#[derive(Debug)]
pub enum ExperimentArtifactError {
    Json(serde_json::Error),
    Invalid(InvalidExperimentReport),
    Io(std::io::Error),
}

impl fmt::Display for ExperimentArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "invalid experiment JSON: {error}"),
            Self::Invalid(error) => write!(formatter, "invalid experiment report: {error}"),
            Self::Io(error) => write!(formatter, "experiment artifact I/O failed: {error}"),
        }
    }
}

impl Error for ExperimentArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Invalid(error) => Some(error),
            Self::Io(error) => Some(error),
        }
    }
}

impl From<serde_json::Error> for ExperimentArtifactError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<InvalidExperimentReport> for ExperimentArtifactError {
    fn from(error: InvalidExperimentReport) -> Self {
        Self::Invalid(error)
    }
}

impl From<std::io::Error> for ExperimentArtifactError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
