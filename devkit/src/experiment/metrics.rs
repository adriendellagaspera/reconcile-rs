use serde::{Deserialize, Serialize};

use super::model::{CostOwner, LifecyclePhase};
use super::validate::ContractError;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Unit {
    Seconds,
    Bytes,
    Count,
    Ratio,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "unit", content = "value", rename_all = "kebab-case")]
pub enum MetricValue {
    Seconds(f64),
    Bytes(u64),
    Count(u64),
    Ratio(f64),
}

impl MetricValue {
    fn unit(self) -> Unit {
        match self {
            Self::Seconds(_) => Unit::Seconds,
            Self::Bytes(_) => Unit::Bytes,
            Self::Count(_) => Unit::Count,
            Self::Ratio(_) => Unit::Ratio,
        }
    }

    fn finite(self) -> bool {
        match self {
            Self::Seconds(value) | Self::Ratio(value) => value.is_finite(),
            Self::Bytes(_) | Self::Count(_) => true,
        }
    }

    fn ratio(self) -> Option<f64> {
        match self {
            Self::Ratio(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Uncertainty {
    pub lower: f64,
    pub upper: f64,
    pub confidence: f64,
}

impl Uncertainty {
    fn valid(self) -> bool {
        self.lower.is_finite()
            && self.upper.is_finite()
            && self.lower <= self.upper
            && self.confidence.is_finite()
            && (0.0..=1.0).contains(&self.confidence)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MissingReason {
    Unknown,
    NotApplicable,
    Unsupported,
    NotMeasured,
    Failed,
    TimedOut,
    Censored,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "provenance", rename_all = "kebab-case")]
pub enum Measurement {
    Observed {
        value: MetricValue,
        samples: u32,
        uncertainty: Option<Uncertainty>,
    },
    Projected {
        value: MetricValue,
        model_id: String,
        inputs_id: String,
    },
    Missing {
        unit: Unit,
        reason: MissingReason,
    },
}

impl Measurement {
    pub(super) fn unit(&self) -> Unit {
        match self {
            Self::Observed { value, .. } | Self::Projected { value, .. } => value.unit(),
            Self::Missing { unit, .. } => *unit,
        }
    }

    pub(super) fn value(&self) -> Option<MetricValue> {
        match self {
            Self::Observed { value, .. } | Self::Projected { value, .. } => Some(*value),
            Self::Missing { .. } => None,
        }
    }

    pub(super) fn is_missing(&self) -> bool {
        matches!(self, Self::Missing { .. })
    }

    pub(super) fn validate_nonnegative_seconds(&self) -> Result<(), ContractError> {
        if matches!(self.value(), Some(MetricValue::Seconds(value)) if value < 0.0) {
            return invalid("duration must be nonnegative");
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::Observed {
                value,
                samples,
                uncertainty,
            } => {
                if !value.finite() {
                    return invalid("observed value must be finite");
                }
                if *samples == 0 {
                    return invalid("observed measurement must have at least one sample");
                }
                if uncertainty.is_some_and(|value| !value.valid()) {
                    return invalid("observed uncertainty is invalid");
                }
            }
            Self::Projected {
                value,
                model_id,
                inputs_id,
            } => {
                if !value.finite() {
                    return invalid("projected value must be finite");
                }
                if model_id.is_empty() || inputs_id.is_empty() {
                    return invalid("projected measurement requires model and input ids");
                }
            }
            Self::Missing { .. } => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MetricKind {
    LocalElapsedSeconds,
    CpuSeconds,
    IoReadBytes,
    IoWriteBytes,
    NetworkPayloadBytes,
    NetworkControlBytes,
    DependentRounds,
    Retries,
    PeakTemporaryBytes,
    PersistentMemoryBytes,
    PersistentDiskBytes,
    ForegroundThroughputDeltaRatio,
    ForegroundP95DeltaSeconds,
    ForegroundP99DeltaSeconds,
    FreshnessLagSeconds,
    FailureProbability,
}

impl MetricKind {
    fn unit(self) -> Unit {
        match self {
            Self::LocalElapsedSeconds
            | Self::CpuSeconds
            | Self::ForegroundP95DeltaSeconds
            | Self::ForegroundP99DeltaSeconds
            | Self::FreshnessLagSeconds => Unit::Seconds,
            Self::IoReadBytes
            | Self::IoWriteBytes
            | Self::NetworkPayloadBytes
            | Self::NetworkControlBytes
            | Self::PeakTemporaryBytes
            | Self::PersistentMemoryBytes
            | Self::PersistentDiskBytes => Unit::Bytes,
            Self::DependentRounds | Self::Retries => Unit::Count,
            Self::ForegroundThroughputDeltaRatio | Self::FailureProbability => Unit::Ratio,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CostMetric {
    pub kind: MetricKind,
    pub peer: Option<String>,
    pub measurement: Measurement,
}

impl CostMetric {
    pub(super) fn validate(&self) -> Result<(), ContractError> {
        self.measurement.validate()?;
        if self.measurement.unit() != self.kind.unit() {
            return Err(ContractError::InvalidMetric(format!(
                "{:?} requires {:?}, found {:?}",
                self.kind,
                self.kind.unit(),
                self.measurement.unit()
            )));
        }
        if matches!(
            self.kind,
            MetricKind::LocalElapsedSeconds
                | MetricKind::CpuSeconds
                | MetricKind::FreshnessLagSeconds
        ) {
            self.measurement.validate_nonnegative_seconds()?;
        }
        if self.kind == MetricKind::FailureProbability {
            if let Some(value) = self.measurement.value().and_then(MetricValue::ratio) {
                if !(0.0..=1.0).contains(&value) {
                    return invalid("failure probability must be between zero and one");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CostRecord {
    pub phase: LifecyclePhase,
    pub owner: CostOwner,
    pub metrics: Vec<CostMetric>,
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ContractError> {
    Err(ContractError::InvalidMetric(message.into()))
}
