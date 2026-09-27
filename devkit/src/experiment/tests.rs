use std::collections::BTreeMap;

use super::*;

mod domains;

#[test]
fn completed_run_requires_every_declared_capability() {
    let experiment = experiment();
    let mut architecture = architecture();
    architecture
        .capabilities
        .insert(Capability::Lookup, CapabilityState::Unsupported);
    let observation = run(RunStatus::Completed);

    assert_eq!(
        validate_observation(&experiment, &architecture, &observation, &[]),
        Err(ContractError::CompletedWithoutCapability(
            Capability::Lookup
        ))
    );

    let disabled = run(RunStatus::Disabled);
    assert!(validate_observation(&experiment, &architecture, &disabled, &[]).is_ok());
    assert_eq!(
        validate_ranking_candidate(&experiment, &architecture, &disabled, &[]),
        Err(RankingError::NotCompleted(RunStatus::Disabled))
    );
}

#[test]
fn experiment_schema_version_is_part_of_the_contract() {
    let mut experiment = experiment();
    experiment.schema_version += 1;
    assert_eq!(
        validate_observation(
            &experiment,
            &architecture(),
            &run(RunStatus::Completed),
            &[]
        ),
        Err(ContractError::UnsupportedSchemaVersion(
            EXPERIMENT_SCHEMA_VERSION + 1
        ))
    );
}

#[test]
fn prepared_state_must_match_architecture_and_logical_view() {
    let experiment = experiment();
    let architecture = architecture();
    let mut observation = run(RunStatus::Completed);
    observation.prepared_state_refs.push("summary".to_owned());

    let mut state = prepared_state();
    state.logical_view_id = "stale-view".to_owned();
    assert_eq!(
        validate_observation(&experiment, &architecture, &observation, &[state]),
        Err(ContractError::PreparedStateViewMismatch(
            "summary".to_owned()
        ))
    );

    let mut state = prepared_state();
    state.architecture_id = "other-architecture".to_owned();
    assert_eq!(
        validate_observation(&experiment, &architecture, &observation, &[state]),
        Err(ContractError::PreparedStateArchitectureMismatch(
            "summary".to_owned()
        ))
    );
}

#[test]
fn reusable_state_can_charge_multiple_owners_but_only_build_phase() {
    let experiment = experiment();
    let architecture = architecture();
    let mut observation = run(RunStatus::Completed);
    observation.prepared_state_refs.push("summary".to_owned());

    let mut state = prepared_state();
    state.build_costs.push(CostRecord {
        phase: LifecyclePhase::InitialArchitectureBuild,
        owner: CostOwner::BaseStore,
        metrics: vec![missing_metric(MetricKind::IoReadBytes, Unit::Bytes)],
    });
    assert!(validate_observation(&experiment, &architecture, &observation, &[state]).is_ok());

    let mut state = prepared_state();
    state.build_costs[0].phase = LifecyclePhase::SessionPreparation;
    assert_eq!(
        validate_observation(&experiment, &architecture, &observation, &[state]),
        Err(ContractError::PreparedStateBuildPhase("summary".to_owned()))
    );
}

#[test]
fn missing_is_distinct_from_an_observed_zero() {
    let missing = CostMetric {
        kind: MetricKind::NetworkPayloadBytes,
        peer: None,
        measurement: Measurement::Missing {
            unit: Unit::Bytes,
            reason: MissingReason::NotMeasured,
        },
    };
    let zero = CostMetric {
        kind: MetricKind::NetworkPayloadBytes,
        peer: None,
        measurement: Measurement::Observed {
            value: MetricValue::Bytes(0),
            samples: 1,
            uncertainty: None,
        },
    };

    let mut observation = run(RunStatus::Completed);
    observation.costs[0].metrics = vec![missing];
    assert!(validate_observation(&experiment(), &architecture(), &observation, &[]).is_ok());

    observation.costs[0].metrics = vec![zero];
    assert!(validate_observation(&experiment(), &architecture(), &observation, &[]).is_ok());
}

#[test]
fn metric_units_and_provenance_are_validated() {
    let mut observation = run(RunStatus::Completed);
    observation.costs[0].metrics = vec![CostMetric {
        kind: MetricKind::IoReadBytes,
        peer: Some("left".to_owned()),
        measurement: Measurement::Observed {
            value: MetricValue::Seconds(1.0),
            samples: 1,
            uncertainty: None,
        },
    }];
    assert_invalid_metric(&observation);

    observation.costs[0].metrics = vec![CostMetric {
        kind: MetricKind::CpuSeconds,
        peer: Some("left".to_owned()),
        measurement: Measurement::Observed {
            value: MetricValue::Seconds(1.0),
            samples: 0,
            uncertainty: None,
        },
    }];
    assert_invalid_metric(&observation);

    observation.costs[0].metrics = vec![CostMetric {
        kind: MetricKind::NetworkPayloadBytes,
        peer: None,
        measurement: Measurement::Projected {
            value: MetricValue::Bytes(1),
            model_id: String::new(),
            inputs_id: "measured-lan".to_owned(),
        },
    }];
    assert_invalid_metric(&observation);
}

#[test]
fn invalid_uncertainty_bounds_are_rejected() {
    for uncertainty in [
        Uncertainty {
            lower: 0.0,
            upper: 1.0,
            confidence: 1.1,
        },
        Uncertainty {
            lower: 2.0,
            upper: 1.0,
            confidence: 0.95,
        },
        Uncertainty {
            lower: 0.0,
            upper: f64::NAN,
            confidence: 0.95,
        },
    ] {
        let mut observation = run(RunStatus::Completed);
        observation.costs[0].metrics = vec![CostMetric {
            kind: MetricKind::CpuSeconds,
            peer: None,
            measurement: Measurement::Observed {
                value: MetricValue::Seconds(1.0),
                samples: 1,
                uncertainty: Some(uncertainty),
            },
        }];
        assert_invalid_metric(&observation);
    }
}

#[test]
fn failure_probability_is_a_probability_not_an_arbitrary_ratio() {
    let mut observation = run(RunStatus::Completed);
    observation.costs[0].metrics = vec![CostMetric {
        kind: MetricKind::FailureProbability,
        peer: None,
        measurement: Measurement::Observed {
            value: MetricValue::Ratio(1.01),
            samples: 1,
            uncertainty: None,
        },
    }];
    assert_invalid_metric(&observation);
}

#[test]
fn duplicate_metrics_and_prepared_state_references_are_rejected() {
    let mut observation = run(RunStatus::Completed);
    let metric = CostMetric {
        kind: MetricKind::CpuSeconds,
        peer: Some("left".to_owned()),
        measurement: observed_seconds(1.0),
    };
    observation.costs[0].metrics = vec![metric.clone(), metric];
    assert_eq!(
        validate_observation(&experiment(), &architecture(), &observation, &[]),
        Err(ContractError::DuplicateMetric)
    );

    let mut observation = run(RunStatus::Completed);
    observation.prepared_state_refs = vec!["summary".to_owned(), "summary".to_owned()];
    assert_eq!(
        validate_observation(
            &experiment(),
            &architecture(),
            &observation,
            &[prepared_state()]
        ),
        Err(ContractError::DuplicatePreparedStateRef(
            "summary".to_owned()
        ))
    );
}

#[test]
fn empty_costs_cannot_make_preparation_look_free() {
    let mut state = prepared_state();
    state.build_costs.clear();
    let mut observation = run(RunStatus::Completed);
    observation.prepared_state_refs.push("summary".to_owned());
    assert_eq!(
        validate_observation(&experiment(), &architecture(), &observation, &[state]),
        Err(ContractError::EmptyPreparedStateCost("summary".to_owned()))
    );
}

#[test]
fn only_completed_observations_with_elapsed_can_rank() {
    for status in [
        RunStatus::Disabled,
        RunStatus::Failed,
        RunStatus::TimedOut,
        RunStatus::Censored,
    ] {
        let observation = run(status);
        assert_eq!(
            validate_ranking_candidate(&experiment(), &architecture(), &observation, &[]),
            Err(RankingError::NotCompleted(status))
        );
    }

    let mut observation = run(RunStatus::Completed);
    observation.end_to_end_elapsed = Measurement::Missing {
        unit: Unit::Seconds,
        reason: MissingReason::NotMeasured,
    };
    assert_eq!(
        validate_ranking_candidate(&experiment(), &architecture(), &observation, &[]),
        Err(RankingError::MissingRankingMetric("end-to-end-elapsed"))
    );
}

#[test]
fn ranking_requires_the_same_task_view_and_completion_boundary() {
    let left_experiment = experiment();
    let mut right_experiment = experiment();
    right_experiment.completion_boundary = CompletionBoundary::LogicalViewVerified;
    let architecture = architecture();
    let left = run(RunStatus::Completed);
    let right = run(RunStatus::Completed);

    assert_eq!(
        validate_pair_for_ranking(
            (&left_experiment, &architecture, &left, &[]),
            (&right_experiment, &architecture, &right, &[])
        ),
        Err(RankingError::IncompatibleExperiments)
    );
}

#[test]
fn end_to_end_elapsed_is_not_a_phase_duration_or_byte_count() {
    let mut observation = run(RunStatus::Completed);
    observation.end_to_end_elapsed = Measurement::Observed {
        value: MetricValue::Bytes(10),
        samples: 1,
        uncertainty: None,
    };
    assert_invalid_metric(&observation);
}

#[test]
fn contract_types_round_trip_as_structured_json() {
    let observation = run(RunStatus::Completed);
    let json = serde_json::to_vec(&observation).unwrap();
    assert_eq!(
        serde_json::from_slice::<RunObservation>(&json).unwrap(),
        observation
    );

    let mut value = serde_json::to_value(&observation).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("human_stdout".to_owned(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<RunObservation>(value).is_err());
}

fn experiment() -> ExperimentSpec {
    ExperimentSpec {
        schema_version: EXPERIMENT_SCHEMA_VERSION,
        id: "redis-g2-500".to_owned(),
        workload_id: "scan.redis72.g1g2".to_owned(),
        logical_task_id: "exact-difference-id-resolution".to_owned(),
        logical_view: LogicalViewSpec {
            id: "frozen-after-mutation".to_owned(),
            semantics: ViewSemantics::Frozen,
            cutoff: None,
        },
        completion_boundary: CompletionBoundary::IdsResolved,
        resource_profile_id: "pilot-host".to_owned(),
        transport_profile_id: "10mbps-rtt28.8ms".to_owned(),
    }
}

fn architecture() -> ArchitectureSpec {
    ArchitectureSpec {
        id: "redis-self-sizing".to_owned(),
        backend: ComponentRevision {
            name: "redis".to_owned(),
            version: "7.2.10".to_owned(),
        },
        addon: Some(ComponentRevision {
            name: "self-sizing-iblt".to_owned(),
            version: "v1".to_owned(),
        }),
        protocol: ComponentRevision {
            name: "self-sizing-iblt".to_owned(),
            version: "v1".to_owned(),
        },
        required_capabilities: vec![Capability::Scan, Capability::Lookup],
        capabilities: BTreeMap::from([
            (Capability::Scan, CapabilityState::Native),
            (Capability::Lookup, CapabilityState::Native),
        ]),
    }
}

fn run(status: RunStatus) -> RunObservation {
    RunObservation {
        experiment_id: "redis-g2-500".to_owned(),
        architecture_id: "redis-self-sizing".to_owned(),
        trial: 0,
        seed: 7,
        actual_logical_view_id: "frozen-after-mutation".to_owned(),
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
        status,
    }
}

fn prepared_state() -> PreparedState {
    PreparedState {
        id: "summary".to_owned(),
        architecture_id: "redis-self-sizing".to_owned(),
        logical_view_id: "frozen-after-mutation".to_owned(),
        build_costs: vec![CostRecord {
            phase: LifecyclePhase::InitialArchitectureBuild,
            owner: CostOwner::Addon,
            metrics: vec![CostMetric {
                kind: MetricKind::CpuSeconds,
                peer: Some("left".to_owned()),
                measurement: observed_seconds(1.0),
            }],
        }],
    }
}

fn observed_seconds(value: f64) -> Measurement {
    Measurement::Observed {
        value: MetricValue::Seconds(value),
        samples: 1,
        uncertainty: None,
    }
}

fn missing_metric(kind: MetricKind, unit: Unit) -> CostMetric {
    CostMetric {
        kind,
        peer: None,
        measurement: Measurement::Missing {
            unit,
            reason: MissingReason::NotMeasured,
        },
    }
}

fn assert_invalid_metric(observation: &RunObservation) {
    assert!(matches!(
        validate_observation(&experiment(), &architecture(), observation, &[]),
        Err(ContractError::InvalidMetric(_))
    ));
}
