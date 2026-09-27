use std::collections::{BTreeMap, BTreeSet};

use super::accounting::{auxiliary_summary_state, native_summary_state, scan_session_cost};
use super::*;
use crate::experiment::{
    lifecycle_cost_ledger, validate_observation, ArchitectureSpec, Capability, CapabilityState,
    CompletionBoundary, ComponentRevision, CostMetric, CostOwner, CostRecord, ExperimentSpec,
    LifecyclePhase, LogicalViewSpec, Measurement, MetricKind, MissingReason, RunObservation,
    RunStatus, Unit, ViewSemantics, EXPERIMENT_SCHEMA_VERSION,
};

#[test]
fn both_backends_expose_the_same_logical_fixture() {
    for case in cases() {
        let fixture = fixture(case);
        let scan_left = fixture.left_scan.scan(&fixture.view_id).unwrap();
        let scan_right = fixture.right_scan.scan(&fixture.view_id).unwrap();
        let ordered_left = fixture.left_ordered.range(&fixture.view_id, 0, 10).unwrap();
        let ordered_right = fixture
            .right_ordered
            .range(&fixture.view_id, 0, 10)
            .unwrap();

        assert_eq!(scan_left, ordered_left);
        assert_eq!(scan_right, ordered_right);
    }
}

#[test]
fn exact_reference_verifies_ids_and_payload_recovery_without_becoming_runtime_input() {
    for case in cases() {
        let fixture = fixture(case);
        let left = fixture.left_scan.scan(&fixture.view_id).unwrap();
        let right = fixture.right_scan.scan(&fixture.view_id).unwrap();
        let ids = difference_ids(&left, &right);
        assert!(fixture.verify_difference_ids(&ids));

        let payloads = fixture.right_scan.lookup(&fixture.view_id, &ids).unwrap();
        assert!(fixture.verify_right_payloads(&ids, &payloads));

        let ordered_payloads = fixture
            .right_ordered
            .lookup(&fixture.view_id, &ids)
            .unwrap();
        assert_eq!(payloads, ordered_payloads);
    }
}

#[test]
fn summaries_detect_all_frozen_fixture_differences_including_equal_count_updates() {
    for case in cases() {
        let fixture = fixture(case);
        let left = fixture
            .left_ordered
            .summary(&fixture.view_id, 0, 10)
            .unwrap();
        let right = fixture
            .right_ordered
            .summary(&fixture.view_id, 0, 10)
            .unwrap();
        assert_ne!(left, right);
    }
}

#[test]
fn stale_views_are_rejected_before_scan_range_summary_or_lookup() {
    let fixture = fixture(DifferenceCase::EqualCountUpdate);
    let ids = BTreeSet::from([2]);

    assert_eq!(fixture.left_scan.scan("stale"), Err(ViewError::Mismatch));
    assert_eq!(
        fixture.left_scan.lookup("stale", &ids),
        Err(ViewError::Mismatch)
    );
    assert_eq!(
        fixture.left_ordered.range("stale", 0, 10),
        Err(ViewError::Mismatch)
    );
    assert_eq!(
        fixture.left_ordered.summary("stale", 0, 10),
        Err(ViewError::Mismatch)
    );
}

#[test]
fn scan_and_native_summary_architectures_declare_different_feasible_capabilities() {
    let fixture = fixture(DifferenceCase::Insert);
    let scan = scan_architecture();
    let ordered = ordered_architecture();

    let scan_run = observation(
        &scan.id,
        &fixture.view_id,
        Vec::new(),
        vec![scan_session_cost()],
    );
    assert!(validate_observation(&experiment(&fixture.view_id), &scan, &scan_run, &[]).is_ok());

    let native = native_summary_state(&ordered.id, &fixture.view_id);
    let ordered_run = observation(
        &ordered.id,
        &fixture.view_id,
        vec![native.id.clone()],
        vec![protocol_session_cost()],
    );
    assert!(validate_observation(
        &experiment(&fixture.view_id),
        &ordered,
        &ordered_run,
        &[native]
    )
    .is_ok());

    let mut impossible_scan = scan;
    impossible_scan
        .required_capabilities
        .push(Capability::Summary);
    assert!(validate_observation(
        &experiment(&fixture.view_id),
        &impossible_scan,
        &scan_run,
        &[]
    )
    .is_err());
}

#[test]
fn auxiliary_summary_build_charges_base_scan_and_addon_separately() {
    let fixture = fixture(DifferenceCase::Delete);
    let state = auxiliary_summary_state("scan-plus-summary", &fixture.view_id);

    assert_eq!(state.build_costs.len(), 2);
    assert_eq!(state.build_costs[0].owner, CostOwner::BaseStore);
    assert_eq!(state.build_costs[1].owner, CostOwner::Addon);
    assert!(state
        .build_costs
        .iter()
        .all(|cost| cost.phase == LifecyclePhase::InitialArchitectureBuild));
    assert!(state
        .build_costs
        .iter()
        .flat_map(|cost| &cost.metrics)
        .all(|metric| matches!(
            metric.measurement,
            Measurement::Missing {
                reason: MissingReason::NotMeasured,
                ..
            }
        )));
}

#[test]
fn reused_preparation_is_charged_once_across_multiple_sessions() {
    let fixture = fixture(DifferenceCase::DivergentVersion);
    let architecture = ordered_architecture();
    let state = native_summary_state(&architecture.id, &fixture.view_id);

    let first = observation(
        &architecture.id,
        &fixture.view_id,
        vec![state.id.clone()],
        vec![protocol_session_cost()],
    );
    let second = observation(
        &architecture.id,
        &fixture.view_id,
        vec![state.id.clone()],
        vec![protocol_session_cost()],
    );
    let ledger = lifecycle_cost_ledger(&[first, second], &[state]).unwrap();

    assert_eq!(
        ledger
            .iter()
            .filter(|cost| cost.phase == LifecyclePhase::InitialArchitectureBuild)
            .count(),
        1
    );
    assert_eq!(
        ledger
            .iter()
            .filter(|cost| cost.phase == LifecyclePhase::SessionWork)
            .count(),
        2
    );
}

fn cases() -> [DifferenceCase; 4] {
    [
        DifferenceCase::Insert,
        DifferenceCase::EqualCountUpdate,
        DifferenceCase::Delete,
        DifferenceCase::DivergentVersion,
    ]
}

fn experiment(view_id: &str) -> ExperimentSpec {
    ExperimentSpec {
        schema_version: EXPERIMENT_SCHEMA_VERSION,
        id: format!("fixture-{view_id}"),
        workload_id: "deterministic-datastore-fixture".to_owned(),
        logical_task_id: "exact-difference-id-resolution".to_owned(),
        logical_view: LogicalViewSpec {
            id: view_id.to_owned(),
            semantics: ViewSemantics::Frozen,
            cutoff: None,
        },
        completion_boundary: CompletionBoundary::IdsResolved,
        resource_profile_id: "deterministic".to_owned(),
        transport_profile_id: "none".to_owned(),
    }
}

fn scan_architecture() -> ArchitectureSpec {
    ArchitectureSpec {
        id: "scan-only".to_owned(),
        backend: component("scan-fixture"),
        addon: None,
        protocol: component("exact-scan"),
        required_capabilities: vec![
            Capability::Scan,
            Capability::Lookup,
            Capability::ConsistentView,
        ],
        capabilities: BTreeMap::from([
            (Capability::Scan, CapabilityState::Native),
            (Capability::Lookup, CapabilityState::Native),
            (Capability::ConsistentView, CapabilityState::Native),
            (Capability::Summary, CapabilityState::Unsupported),
        ]),
    }
}

fn ordered_architecture() -> ArchitectureSpec {
    ArchitectureSpec {
        id: "ordered-native-summary".to_owned(),
        backend: component("ordered-fixture"),
        addon: None,
        protocol: component("range-summary"),
        required_capabilities: vec![
            Capability::RangeScan,
            Capability::Lookup,
            Capability::Summary,
            Capability::ConsistentView,
        ],
        capabilities: BTreeMap::from([
            (Capability::RangeScan, CapabilityState::Native),
            (Capability::Lookup, CapabilityState::Native),
            (Capability::Summary, CapabilityState::Native),
            (Capability::ConsistentView, CapabilityState::Native),
        ]),
    }
}

fn component(name: &str) -> ComponentRevision {
    ComponentRevision {
        name: name.to_owned(),
        version: "fixture-v1".to_owned(),
    }
}

fn observation(
    architecture_id: &str,
    view_id: &str,
    prepared_state_refs: Vec<String>,
    costs: Vec<CostRecord>,
) -> RunObservation {
    RunObservation {
        experiment_id: format!("fixture-{view_id}"),
        architecture_id: architecture_id.to_owned(),
        trial: 0,
        seed: 0,
        actual_logical_view_id: view_id.to_owned(),
        prepared_state_refs,
        costs,
        end_to_end_elapsed: Measurement::Missing {
            unit: Unit::Seconds,
            reason: MissingReason::NotMeasured,
        },
        status: RunStatus::Completed,
    }
}

fn protocol_session_cost() -> CostRecord {
    CostRecord {
        phase: LifecyclePhase::SessionWork,
        owner: CostOwner::Protocol,
        metrics: vec![CostMetric {
            kind: MetricKind::CpuSeconds,
            peer: None,
            measurement: Measurement::Missing {
                unit: Unit::Seconds,
                reason: MissingReason::NotMeasured,
            },
        }],
    }
}
