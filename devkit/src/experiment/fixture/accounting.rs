use crate::experiment::{
    CostMetric, CostOwner, CostRecord, LifecyclePhase, Measurement, MetricKind, MissingReason,
    PreparedState, Unit,
};

pub(super) fn auxiliary_summary_state(architecture_id: &str, view_id: &str) -> PreparedState {
    PreparedState {
        id: format!("{architecture_id}-aux-summary"),
        architecture_id: architecture_id.to_owned(),
        logical_view_id: view_id.to_owned(),
        build_costs: vec![
            unknown_cost(
                LifecyclePhase::InitialArchitectureBuild,
                CostOwner::BaseStore,
                MetricKind::IoReadBytes,
                Unit::Bytes,
            ),
            CostRecord {
                phase: LifecyclePhase::InitialArchitectureBuild,
                owner: CostOwner::Addon,
                metrics: vec![
                    missing_metric(MetricKind::CpuSeconds, Unit::Seconds),
                    missing_metric(MetricKind::PersistentMemoryBytes, Unit::Bytes),
                ],
            },
        ],
    }
}

pub(super) fn native_summary_state(architecture_id: &str, view_id: &str) -> PreparedState {
    PreparedState {
        id: format!("{architecture_id}-native-summary"),
        architecture_id: architecture_id.to_owned(),
        logical_view_id: view_id.to_owned(),
        build_costs: vec![CostRecord {
            phase: LifecyclePhase::InitialArchitectureBuild,
            owner: CostOwner::BaseStore,
            metrics: vec![
                missing_metric(MetricKind::CpuSeconds, Unit::Seconds),
                missing_metric(MetricKind::PersistentDiskBytes, Unit::Bytes),
            ],
        }],
    }
}

pub(super) fn scan_session_cost() -> CostRecord {
    unknown_cost(
        LifecyclePhase::SessionPreparation,
        CostOwner::BaseStore,
        MetricKind::IoReadBytes,
        Unit::Bytes,
    )
}

fn unknown_cost(
    phase: LifecyclePhase,
    owner: CostOwner,
    kind: MetricKind,
    unit: Unit,
) -> CostRecord {
    CostRecord {
        phase,
        owner,
        metrics: vec![missing_metric(kind, unit)],
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
