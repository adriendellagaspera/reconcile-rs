//! Shared experiment identity, lifecycle ledger and validated machine artifacts.
mod artifact;
mod metrics;
mod model;
pub mod producer;
mod render;
mod trace;
mod validate;
pub use render::write_experiment_summary;
pub use trace::{
    read_repair_trace, write_repair_trace, PeerSide, RepairStage, RepairStrategy, RepairTrace,
    REPAIR_TRACE_SCHEMA_VERSION,
};

pub use artifact::{
    join_experiment_reports, read_experiment_report, validate_experiment_report,
    write_experiment_report, AnalysisContext, ExperimentArtifactError, ExperimentReport,
    InvalidExperimentReport, EXPERIMENT_REPORT_SCHEMA_VERSION,
};
pub use metrics::{
    CostMetric, CostRecord, Measurement, MetricKind, MetricValue, MissingReason, Uncertainty, Unit,
};
pub use model::{
    ArchitectureSpec, Capability, CapabilityState, CompletionBoundary, ComponentRevision,
    CostOwner, ExperimentSpec, LifecyclePhase, LogicalViewSpec, PreparedState, RunObservation,
    RunStatus, ViewSemantics, EXPERIMENT_SCHEMA_VERSION,
};
pub use validate::{
    lifecycle_cost_ledger, validate_observation, validate_pair_for_ranking,
    validate_ranking_candidate, ContractError, RankingError,
};

#[cfg(test)]
mod fixture;
#[cfg(test)]
mod tests;
