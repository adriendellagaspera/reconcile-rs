use std::collections::BTreeSet;

use super::metrics::{CostMetric, CostRecord, Unit};
use super::model::{
    ArchitectureSpec, Capability, ExperimentSpec, LifecyclePhase, PreparedState, RunObservation,
    RunStatus, EXPERIMENT_SCHEMA_VERSION,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContractError {
    UnsupportedSchemaVersion(u32),
    ExperimentIdentityMismatch,
    ArchitectureIdentityMismatch,
    LogicalViewMismatch,
    CompletedWithoutCapability(Capability),
    DuplicatePreparedStateId(String),
    DuplicatePreparedStateRef(String),
    UnknownPreparedState(String),
    PreparedStateArchitectureMismatch(String),
    PreparedStateViewMismatch(String),
    PreparedStateBuildPhase(String),
    EmptyPreparedStateCost(String),
    EmptyCostRecord,
    InvalidMetric(String),
    DuplicateMetric,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankingError {
    Contract(ContractError),
    NotCompleted(RunStatus),
    IncompatibleExperiments,
    MissingRankingMetric(&'static str),
}

impl CostRecord {
    fn validate(&self) -> Result<(), ContractError> {
        if self.metrics.is_empty() {
            return Err(ContractError::EmptyCostRecord);
        }
        let mut keys = BTreeSet::new();
        for metric in &self.metrics {
            metric.validate()?;
            if !keys.insert(metric_key(metric)) {
                return Err(ContractError::DuplicateMetric);
            }
        }
        Ok(())
    }
}

fn metric_key(metric: &CostMetric) -> (super::metrics::MetricKind, Option<&str>) {
    (metric.kind, metric.peer.as_deref())
}

pub fn validate_observation(
    experiment: &ExperimentSpec,
    architecture: &ArchitectureSpec,
    observation: &RunObservation,
    prepared_states: &[PreparedState],
) -> Result<(), ContractError> {
    if experiment.schema_version != EXPERIMENT_SCHEMA_VERSION {
        return Err(ContractError::UnsupportedSchemaVersion(
            experiment.schema_version,
        ));
    }
    if observation.experiment_id != experiment.id {
        return Err(ContractError::ExperimentIdentityMismatch);
    }
    if observation.architecture_id != architecture.id {
        return Err(ContractError::ArchitectureIdentityMismatch);
    }
    if observation.actual_logical_view_id != experiment.logical_view.id {
        return Err(ContractError::LogicalViewMismatch);
    }
    if let (RunStatus::Completed, Some(capability)) = (
        observation.status,
        architecture.first_unavailable_capability(),
    ) {
        return Err(ContractError::CompletedWithoutCapability(capability));
    }

    if observation.end_to_end_elapsed.unit() != Unit::Seconds {
        return Err(ContractError::InvalidMetric(
            "end-to-end elapsed must use seconds".to_owned(),
        ));
    }
    observation.end_to_end_elapsed.validate()?;
    observation
        .end_to_end_elapsed
        .validate_nonnegative_seconds()?;
    for cost in &observation.costs {
        cost.validate()?;
    }
    validate_prepared_states(experiment, architecture, observation, prepared_states)
}

fn validate_prepared_states(
    experiment: &ExperimentSpec,
    architecture: &ArchitectureSpec,
    observation: &RunObservation,
    prepared_states: &[PreparedState],
) -> Result<(), ContractError> {
    let mut state_ids = BTreeSet::new();
    for state in prepared_states {
        if !state_ids.insert(state.id.as_str()) {
            return Err(ContractError::DuplicatePreparedStateId(state.id.clone()));
        }
        if state.build_costs.is_empty() {
            return Err(ContractError::EmptyPreparedStateCost(state.id.clone()));
        }
        for cost in &state.build_costs {
            cost.validate()?;
            if cost.phase != LifecyclePhase::InitialArchitectureBuild {
                return Err(ContractError::PreparedStateBuildPhase(state.id.clone()));
            }
        }
    }

    let mut refs = BTreeSet::new();
    for state_ref in &observation.prepared_state_refs {
        if !refs.insert(state_ref.as_str()) {
            return Err(ContractError::DuplicatePreparedStateRef(state_ref.clone()));
        }
        let state = prepared_states
            .iter()
            .find(|state| state.id == state_ref.as_str())
            .ok_or_else(|| ContractError::UnknownPreparedState(state_ref.clone()))?;
        if state.architecture_id != architecture.id {
            return Err(ContractError::PreparedStateArchitectureMismatch(
                state_ref.clone(),
            ));
        }
        if state.logical_view_id != experiment.logical_view.id {
            return Err(ContractError::PreparedStateViewMismatch(state_ref.clone()));
        }
    }
    Ok(())
}

pub fn lifecycle_cost_ledger(
    observations: &[RunObservation],
    prepared_states: &[PreparedState],
) -> Result<Vec<CostRecord>, ContractError> {
    let mut state_ids = BTreeSet::new();
    for state in prepared_states {
        if !state_ids.insert(state.id.as_str()) {
            return Err(ContractError::DuplicatePreparedStateId(state.id.clone()));
        }
    }

    let mut referenced = BTreeSet::new();
    let mut ledger = Vec::new();
    for observation in observations {
        for state_ref in &observation.prepared_state_refs {
            if referenced.insert(state_ref.as_str()) {
                let state = prepared_states
                    .iter()
                    .find(|state| state.id == state_ref.as_str())
                    .ok_or_else(|| ContractError::UnknownPreparedState(state_ref.clone()))?;
                if state.build_costs.is_empty() {
                    return Err(ContractError::EmptyPreparedStateCost(state.id.clone()));
                }
                for cost in &state.build_costs {
                    cost.validate()?;
                    if cost.phase != LifecyclePhase::InitialArchitectureBuild {
                        return Err(ContractError::PreparedStateBuildPhase(state.id.clone()));
                    }
                    ledger.push(cost.clone());
                }
            }
        }
        for cost in &observation.costs {
            cost.validate()?;
            ledger.push(cost.clone());
        }
    }
    Ok(ledger)
}

pub fn validate_ranking_candidate(
    experiment: &ExperimentSpec,
    architecture: &ArchitectureSpec,
    observation: &RunObservation,
    prepared_states: &[PreparedState],
) -> Result<(), RankingError> {
    validate_observation(experiment, architecture, observation, prepared_states)
        .map_err(RankingError::Contract)?;
    if observation.status != RunStatus::Completed {
        return Err(RankingError::NotCompleted(observation.status));
    }
    if observation.end_to_end_elapsed.is_missing() {
        return Err(RankingError::MissingRankingMetric("end-to-end-elapsed"));
    }
    Ok(())
}

pub fn validate_pair_for_ranking(
    left: (
        &ExperimentSpec,
        &ArchitectureSpec,
        &RunObservation,
        &[PreparedState],
    ),
    right: (
        &ExperimentSpec,
        &ArchitectureSpec,
        &RunObservation,
        &[PreparedState],
    ),
) -> Result<(), RankingError> {
    validate_ranking_candidate(left.0, left.1, left.2, left.3)?;
    validate_ranking_candidate(right.0, right.1, right.2, right.3)?;
    if !left.0.same_comparison_task(right.0) {
        return Err(RankingError::IncompatibleExperiments);
    }
    Ok(())
}
