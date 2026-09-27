use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use super::{
    validate_observation, ArchitectureSpec, ExperimentSpec, Measurement, PreparedState,
    RunObservation, Unit,
};

mod error;

pub use error::{ExperimentArtifactError, InvalidExperimentReport};

pub const EXPERIMENT_REPORT_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnalysisContext {
    pub logical_records_per_peer: BTreeMap<String, Measurement>,
    pub symmetric_difference_elements: Measurement,
    pub logical_mutations: Measurement,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentReport {
    pub schema_version: u32,
    pub experiment: ExperimentSpec,
    pub analysis_context: AnalysisContext,
    pub architectures: Vec<ArchitectureSpec>,
    pub prepared_states: Vec<PreparedState>,
    pub observations: Vec<RunObservation>,
}

pub fn read_experiment_report(
    reader: impl Read,
) -> Result<ExperimentReport, ExperimentArtifactError> {
    let report = serde_json::from_reader(reader)?;
    validate_experiment_report(&report)?;
    Ok(report)
}

pub fn write_experiment_report(
    mut writer: impl Write,
    report: &ExperimentReport,
) -> Result<(), ExperimentArtifactError> {
    validate_experiment_report(report)?;
    serde_json::to_writer_pretty(&mut writer, report)?;
    writer.write_all(b"\n")?;
    Ok(())
}

pub fn validate_experiment_report(
    report: &ExperimentReport,
) -> Result<(), InvalidExperimentReport> {
    if report.schema_version != EXPERIMENT_REPORT_SCHEMA_VERSION {
        return Err(InvalidExperimentReport::UnsupportedSchemaVersion(
            report.schema_version,
        ));
    }
    validate_context(&report.analysis_context)?;
    if report.architectures.is_empty() {
        return Err(InvalidExperimentReport::EmptyArchitectures);
    }
    if report.observations.is_empty() {
        return Err(InvalidExperimentReport::EmptyObservations);
    }

    let architectures = architecture_map(&report.architectures)?;
    validate_prepared_catalog(report, &architectures)?;
    validate_observations(report, &architectures)
}

fn validate_context(context: &AnalysisContext) -> Result<(), InvalidExperimentReport> {
    if context.logical_records_per_peer.is_empty() {
        return Err(InvalidExperimentReport::EmptyPeerContext);
    }
    for measurement in context.logical_records_per_peer.values() {
        validate_count(measurement, "logical-records-per-peer")?;
    }
    validate_count(
        &context.symmetric_difference_elements,
        "symmetric-difference-elements",
    )?;
    validate_count(&context.logical_mutations, "logical-mutations")
}

fn validate_count(
    measurement: &Measurement,
    label: &'static str,
) -> Result<(), InvalidExperimentReport> {
    measurement
        .validate()
        .map_err(InvalidExperimentReport::Contract)?;
    if measurement.unit() != Unit::Count {
        return Err(InvalidExperimentReport::InvalidContextMetric(label));
    }
    Ok(())
}

fn architecture_map(
    architectures: &[ArchitectureSpec],
) -> Result<BTreeMap<&str, &ArchitectureSpec>, InvalidExperimentReport> {
    let mut by_id = BTreeMap::new();
    for architecture in architectures {
        if by_id
            .insert(architecture.id.as_str(), architecture)
            .is_some()
        {
            return Err(InvalidExperimentReport::DuplicateArchitecture(
                architecture.id.clone(),
            ));
        }
        let mut required = BTreeSet::new();
        for capability in &architecture.required_capabilities {
            if !required.insert(*capability) {
                return Err(InvalidExperimentReport::DuplicateRequiredCapability(
                    architecture.id.clone(),
                    *capability,
                ));
            }
        }
    }
    Ok(by_id)
}

fn validate_prepared_catalog(
    report: &ExperimentReport,
    architectures: &BTreeMap<&str, &ArchitectureSpec>,
) -> Result<(), InvalidExperimentReport> {
    let mut ids = BTreeSet::new();
    for state in &report.prepared_states {
        if !ids.insert(state.id.as_str()) {
            return Err(InvalidExperimentReport::DuplicatePreparedState(
                state.id.clone(),
            ));
        }
        if !architectures.contains_key(state.architecture_id.as_str()) {
            return Err(InvalidExperimentReport::UnknownPreparedStateArchitecture(
                state.id.clone(),
            ));
        }
        if state.logical_view_id != report.experiment.logical_view.id {
            return Err(InvalidExperimentReport::PreparedStateViewMismatch(
                state.id.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_observations(
    report: &ExperimentReport,
    architectures: &BTreeMap<&str, &ArchitectureSpec>,
) -> Result<(), InvalidExperimentReport> {
    let mut identities = BTreeSet::new();
    for observation in &report.observations {
        let architecture = architectures
            .get(observation.architecture_id.as_str())
            .ok_or_else(|| {
                InvalidExperimentReport::UnknownArchitecture(observation.architecture_id.clone())
            })?;
        let identity = (
            observation.architecture_id.as_str(),
            observation.trial,
            observation.seed,
        );
        if !identities.insert(identity) {
            return Err(InvalidExperimentReport::DuplicateObservation(
                observation.architecture_id.clone(),
                observation.trial,
                observation.seed,
            ));
        }
        validate_observation(
            &report.experiment,
            architecture,
            observation,
            &report.prepared_states,
        )?;
    }
    Ok(())
}

pub fn join_experiment_reports(
    reports: &[ExperimentReport],
) -> Result<ExperimentReport, InvalidExperimentReport> {
    let Some(first) = reports.first() else {
        return Err(InvalidExperimentReport::EmptyJoin);
    };
    validate_experiment_report(first)?;
    let mut joined = first.clone();

    for report in &reports[1..] {
        validate_experiment_report(report)?;
        if report.schema_version != joined.schema_version
            || report.experiment != joined.experiment
            || report.analysis_context != joined.analysis_context
        {
            return Err(InvalidExperimentReport::IncompatibleJoin);
        }
        merge_architectures(&mut joined.architectures, &report.architectures)?;
        merge_prepared_states(&mut joined.prepared_states, &report.prepared_states)?;
        joined.observations.extend(report.observations.clone());
    }
    validate_experiment_report(&joined)?;
    Ok(joined)
}

fn merge_architectures(
    target: &mut Vec<ArchitectureSpec>,
    incoming: &[ArchitectureSpec],
) -> Result<(), InvalidExperimentReport> {
    for architecture in incoming {
        match target
            .iter()
            .find(|existing| existing.id == architecture.id)
        {
            Some(existing) if existing != architecture => {
                return Err(InvalidExperimentReport::ConflictingArchitecture(
                    architecture.id.clone(),
                ));
            }
            Some(_) => {}
            None => target.push(architecture.clone()),
        }
    }
    Ok(())
}

fn merge_prepared_states(
    target: &mut Vec<PreparedState>,
    incoming: &[PreparedState],
) -> Result<(), InvalidExperimentReport> {
    for state in incoming {
        match target.iter().find(|existing| existing.id == state.id) {
            Some(existing) if existing != state => {
                return Err(InvalidExperimentReport::ConflictingPreparedState(
                    state.id.clone(),
                ));
            }
            Some(_) => {}
            None => target.push(state.clone()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
