use std::collections::BTreeMap;

use super::*;
use crate::experiment::{
    Capability, CapabilityState, CompletionBoundary, ComponentRevision, CostMetric, CostOwner,
    CostRecord, LifecyclePhase, LogicalViewSpec, Measurement, MetricKind, MetricValue,
    MissingReason, RunStatus, ViewSemantics, EXPERIMENT_SCHEMA_VERSION,
};

#[test]
fn experiment_report_round_trips_without_human_output_parsing() {
    let report = report(0, 7);
    let mut json = Vec::new();
    write_experiment_report(&mut json, &report).unwrap();
    assert_eq!(read_experiment_report(json.as_slice()).unwrap(), report);

    let mut value = serde_json::to_value(&report).unwrap();
    value.as_object_mut().unwrap().insert(
        "stdout".to_owned(),
        serde_json::Value::String("no".to_owned()),
    );
    assert!(serde_json::from_value::<ExperimentReport>(value).is_err());
}

#[test]
fn report_schema_version_is_explicitly_rejected() {
    let mut report = report(0, 7);
    report.schema_version += 1;
    assert_eq!(
        validate_experiment_report(&report),
        Err(InvalidExperimentReport::UnsupportedSchemaVersion(
            EXPERIMENT_REPORT_SCHEMA_VERSION + 1
        ))
    );
}

#[test]
fn analysis_context_is_typed_and_may_preserve_unknown_counts() {
    let report = report(0, 7);
    assert!(validate_experiment_report(&report).is_ok());

    let mut invalid = report;
    invalid.analysis_context.symmetric_difference_elements = observed_seconds(2.0);
    assert_eq!(
        validate_experiment_report(&invalid),
        Err(InvalidExperimentReport::InvalidContextMetric(
            "symmetric-difference-elements"
        ))
    );
}

#[test]
fn duplicate_or_unknown_architecture_identity_is_rejected() {
    let mut duplicate = report(0, 7);
    duplicate
        .architectures
        .push(duplicate.architectures[0].clone());
    assert_eq!(
        validate_experiment_report(&duplicate),
        Err(InvalidExperimentReport::DuplicateArchitecture(
            "scan-only".to_owned()
        ))
    );

    let mut unknown = report(0, 7);
    unknown.observations[0].architecture_id = "not-declared".to_owned();
    assert_eq!(
        validate_experiment_report(&unknown),
        Err(InvalidExperimentReport::UnknownArchitecture(
            "not-declared".to_owned()
        ))
    );
}

#[test]
fn duplicate_required_capabilities_are_rejected() {
    let mut report = report(0, 7);
    report.architectures[0]
        .required_capabilities
        .push(Capability::Scan);
    assert_eq!(
        validate_experiment_report(&report),
        Err(InvalidExperimentReport::DuplicateRequiredCapability(
            "scan-only".to_owned(),
            Capability::Scan
        ))
    );
}

#[test]
fn duplicate_observations_are_rejected_before_analysis() {
    let mut report = report(0, 7);
    report.observations.push(report.observations[0].clone());
    assert_eq!(
        validate_experiment_report(&report),
        Err(InvalidExperimentReport::DuplicateObservation(
            "scan-only".to_owned(),
            0,
            7
        ))
    );
}

#[test]
fn compatible_partial_reports_join_without_duplicating_architecture_metadata() {
    let left = report(0, 7);
    let right = report(1, 8);
    let joined = join_experiment_reports(&[left, right]).unwrap();

    assert_eq!(joined.architectures.len(), 1);
    assert_eq!(joined.observations.len(), 2);
}

#[test]
fn join_rejects_incompatible_task_identity() {
    let left = report(0, 7);
    let mut right = report(1, 8);
    right.experiment.completion_boundary = CompletionBoundary::LogicalViewVerified;

    assert_eq!(
        join_experiment_reports(&[left, right]),
        Err(InvalidExperimentReport::IncompatibleJoin)
    );
}

#[test]
fn join_rejects_duplicate_run_identity() {
    let left = report(0, 7);
    let right = report(0, 7);

    assert_eq!(
        join_experiment_reports(&[left, right]),
        Err(InvalidExperimentReport::DuplicateObservation(
            "scan-only".to_owned(),
            0,
            7
        ))
    );
}

#[test]
fn join_rejects_conflicting_architecture_revision() {
    let left = report(0, 7);
    let mut right = report(1, 8);
    right.architectures[0].backend.version = "fixture-v2".to_owned();

    assert_eq!(
        join_experiment_reports(&[left, right]),
        Err(InvalidExperimentReport::ConflictingArchitecture(
            "scan-only".to_owned()
        ))
    );
}

fn report(trial: u32, seed: u64) -> ExperimentReport {
    ExperimentReport {
        schema_version: EXPERIMENT_REPORT_SCHEMA_VERSION,
        experiment: ExperimentSpec {
            schema_version: EXPERIMENT_SCHEMA_VERSION,
            id: "fixture-run".to_owned(),
            workload_id: "scan.redis72.g1g2".to_owned(),
            logical_task_id: "exact-difference-id-resolution".to_owned(),
            logical_view: LogicalViewSpec {
                id: "frozen".to_owned(),
                semantics: ViewSemantics::Frozen,
                cutoff: None,
            },
            completion_boundary: CompletionBoundary::IdsResolved,
            resource_profile_id: "fixture-host".to_owned(),
            transport_profile_id: "none".to_owned(),
        },
        analysis_context: AnalysisContext {
            logical_records_per_peer: BTreeMap::from([
                ("left".to_owned(), observed_count(4)),
                ("right".to_owned(), observed_count(4)),
            ]),
            symmetric_difference_elements: observed_count(2),
            logical_mutations: Measurement::Missing {
                unit: Unit::Count,
                reason: MissingReason::Unknown,
            },
        },
        architectures: vec![ArchitectureSpec {
            id: "scan-only".to_owned(),
            backend: ComponentRevision {
                name: "scan-fixture".to_owned(),
                version: "fixture-v1".to_owned(),
            },
            addon: None,
            protocol: ComponentRevision {
                name: "exact-scan".to_owned(),
                version: "fixture-v1".to_owned(),
            },
            required_capabilities: vec![Capability::Scan],
            capabilities: BTreeMap::from([(Capability::Scan, CapabilityState::Native)]),
        }],
        prepared_states: Vec::new(),
        observations: vec![RunObservation {
            experiment_id: "fixture-run".to_owned(),
            architecture_id: "scan-only".to_owned(),
            trial,
            seed,
            actual_logical_view_id: "frozen".to_owned(),
            prepared_state_refs: Vec::new(),
            costs: vec![CostRecord {
                phase: LifecyclePhase::SessionWork,
                owner: CostOwner::Protocol,
                metrics: vec![CostMetric {
                    kind: MetricKind::CpuSeconds,
                    peer: Some("left".to_owned()),
                    measurement: observed_seconds(0.1),
                }],
            }],
            end_to_end_elapsed: observed_seconds(0.2),
            status: RunStatus::Completed,
        }],
    }
}

fn observed_count(value: u64) -> Measurement {
    Measurement::Observed {
        value: MetricValue::Count(value),
        samples: 1,
        uncertainty: None,
    }
}

fn observed_seconds(value: f64) -> Measurement {
    Measurement::Observed {
        value: MetricValue::Seconds(value),
        samples: 1,
        uncertainty: None,
    }
}

#[test]
fn prepared_catalog_rejects_unknown_architecture_and_wrong_view() {
    let mut unknown = report(0, 7);
    unknown.prepared_states.push(PreparedState {
        id: "prepared".to_owned(),
        architecture_id: "missing".to_owned(),
        logical_view_id: unknown.experiment.logical_view.id.clone(),
        build_costs: vec![],
    });
    assert_eq!(
        validate_experiment_report(&unknown),
        Err(InvalidExperimentReport::UnknownPreparedStateArchitecture(
            "prepared".to_owned()
        ))
    );

    let mut wrong_view = report(0, 7);
    wrong_view.prepared_states.push(PreparedState {
        id: "prepared".to_owned(),
        architecture_id: wrong_view.architectures[0].id.clone(),
        logical_view_id: "other-view".to_owned(),
        build_costs: vec![],
    });
    assert_eq!(
        validate_experiment_report(&wrong_view),
        Err(InvalidExperimentReport::PreparedStateViewMismatch(
            "prepared".to_owned()
        ))
    );
}

#[test]
fn compatible_prepared_states_deduplicate_and_conflicts_are_rejected() {
    let mut left = report(0, 7);
    let state = PreparedState {
        id: "prepared".to_owned(),
        architecture_id: left.architectures[0].id.clone(),
        logical_view_id: left.experiment.logical_view.id.clone(),
        build_costs: vec![CostRecord {
            phase: LifecyclePhase::InitialArchitectureBuild,
            owner: CostOwner::Addon,
            metrics: vec![CostMetric {
                kind: MetricKind::IoReadBytes,
                peer: None,
                measurement: Measurement::Missing {
                    unit: Unit::Bytes,
                    reason: MissingReason::NotMeasured,
                },
            }],
        }],
    };
    left.prepared_states.push(state.clone());

    let mut right = report(1, 8);
    right.prepared_states.push(state.clone());
    let joined = join_experiment_reports(&[left.clone(), right]).unwrap();
    assert_eq!(joined.prepared_states, vec![state.clone()]);

    let mut conflicting = report(1, 8);
    let mut conflict = state;
    conflict.build_costs[0].metrics[0].measurement = Measurement::Missing {
        unit: Unit::Bytes,
        reason: MissingReason::Unknown,
    };
    conflicting.prepared_states.push(conflict);
    assert_eq!(
        join_experiment_reports(&[left, conflicting]),
        Err(InvalidExperimentReport::ConflictingPreparedState(
            "prepared".to_owned()
        ))
    );
}

#[test]
fn artifact_error_display_preserves_context() {
    let invalid =
        ExperimentArtifactError::Invalid(InvalidExperimentReport::UnsupportedSchemaVersion(99));
    assert_eq!(
        invalid.to_string(),
        "invalid experiment report: UnsupportedSchemaVersion(99)"
    );

    let io = ExperimentArtifactError::Io(std::io::Error::other("fixture I/O failure"));
    assert_eq!(
        io.to_string(),
        "experiment artifact I/O failed: fixture I/O failure"
    );
}

#[test]
fn artifact_error_exposes_underlying_source() {
    use std::error::Error as _;

    let io = ExperimentArtifactError::Io(std::io::Error::other("fixture source"));
    let source = io.source().expect("I/O wrapper must expose its source");
    assert_eq!(source.to_string(), "fixture source");

    let invalid =
        ExperimentArtifactError::Invalid(InvalidExperimentReport::UnsupportedSchemaVersion(99));
    assert!(invalid.source().is_some());
}
