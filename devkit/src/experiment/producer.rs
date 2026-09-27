//! Explicit artifact output for local benchmarks; never reconstructs data from stdout.
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::time::Duration;

use super::*;

use super::write_repair_trace;

pub fn observed(value: MetricValue) -> Measurement {
    Measurement::Observed {
        value,
        samples: 1,
        uncertainty: None,
    }
}

pub fn missing(unit: Unit) -> Measurement {
    Measurement::Missing {
        unit,
        reason: MissingReason::NotMeasured,
    }
}

pub fn elapsed(phase: LifecyclePhase, owner: CostOwner, duration: Duration) -> CostRecord {
    CostRecord {
        phase,
        owner,
        metrics: vec![CostMetric {
            kind: MetricKind::LocalElapsedSeconds,
            peer: None,
            measurement: observed(MetricValue::Seconds(duration.as_secs_f64())),
        }],
    }
}

/// A declared byte-accounting model, excluding transport framing and application repair.
pub fn protocol_bytes(bytes: usize, model: &str, inputs: &str) -> CostRecord {
    CostRecord {
        phase: LifecyclePhase::SessionWork,
        owner: CostOwner::Protocol,
        metrics: vec![CostMetric {
            kind: MetricKind::NetworkPayloadBytes,
            peer: None,
            measurement: Measurement::Projected {
                value: MetricValue::Bytes(bytes as u64),
                model_id: model.to_owned(),
                inputs_id: inputs.to_owned(),
            },
        }],
    }
}

pub struct Arm {
    pub id: String,
    pub implementation: ComponentRevision,
    pub costs: Vec<CostRecord>,
}

impl Arm {
    pub fn new(id: &str, implementation: &str, version: &str, costs: Vec<CostRecord>) -> Self {
        Self {
            id: id.to_owned(),
            implementation: ComponentRevision {
                name: implementation.to_owned(),
                version: version.to_owned(),
            },
            costs,
        }
    }
}

pub struct Case<'a> {
    pub target: &'a str,
    pub workload: &'a str,
    pub seed: u64,
    pub records: BTreeMap<String, usize>,
    pub symmetric_difference: Option<usize>,
}

impl Case<'_> {
    pub fn report(
        self,
        revision: &str,
        resource_profile: &str,
        arms: Vec<Arm>,
    ) -> ExperimentReport {
        let id = format!(
            "{}:{}:seed={}:rev={revision}",
            self.target, self.workload, self.seed
        );
        let mut architectures = Vec::new();
        let mut observations = Vec::new();
        for arm in arms {
            architectures.push(ArchitectureSpec {
                id: arm.id.clone(),
                backend: ComponentRevision {
                    name: "canonical-in-memory-rows".to_owned(),
                    version: revision.to_owned(),
                },
                addon: Some(arm.implementation.clone()),
                protocol: arm.implementation,
                required_capabilities: vec![],
                capabilities: BTreeMap::new(),
            });
            let mut costs = arm.costs;
            costs.push(CostRecord {
                phase: LifecyclePhase::SessionWork,
                owner: CostOwner::Protocol,
                metrics: vec![
                    CostMetric {
                        kind: MetricKind::CpuSeconds,
                        peer: None,
                        measurement: missing(Unit::Seconds),
                    },
                    CostMetric {
                        kind: MetricKind::PeakTemporaryBytes,
                        peer: None,
                        measurement: missing(Unit::Bytes),
                    },
                ],
            });
            observations.push(RunObservation {
                experiment_id: id.clone(),
                architecture_id: arm.id,
                trial: 0,
                seed: self.seed,
                actual_logical_view_id: id.clone(),
                prepared_state_refs: vec![],
                costs,
                end_to_end_elapsed: missing(Unit::Seconds),
                status: RunStatus::Completed,
            });
        }
        ExperimentReport {
            schema_version: EXPERIMENT_REPORT_SCHEMA_VERSION,
            experiment: ExperimentSpec {
                schema_version: EXPERIMENT_SCHEMA_VERSION,
                id: id.clone(),
                workload_id: self.workload.to_owned(),
                logical_task_id: self.target.to_owned(),
                logical_view: LogicalViewSpec {
                    id,
                    semantics: ViewSemantics::Frozen,
                    cutoff: None,
                },
                completion_boundary: CompletionBoundary::DifferenceDiscovered,
                resource_profile_id: resource_profile.to_owned(),
                transport_profile_id: "local-counted-no-transport".to_owned(),
            },
            analysis_context: AnalysisContext {
                logical_records_per_peer: self
                    .records
                    .into_iter()
                    .map(|(peer, count)| (peer, observed(MetricValue::Count(count as u64))))
                    .collect(),
                symmetric_difference_elements: self.symmetric_difference.map_or_else(
                    || missing(Unit::Count),
                    |n| observed(MetricValue::Count(n as u64)),
                ),
                logical_mutations: missing(Unit::Count),
            },
            architectures,
            prepared_states: vec![],
            observations,
        }
    }
}

/// Optional output requires caller-declared revision and host/build profile identifiers.
/// Each file is created once; reusing an output directory never overwrites an observation.
pub fn write_repair_trace_from_env(
    target: &str,
    workload: &str,
    seed: u64,
    arm: &str,
    trace: &RepairTrace,
) {
    let Some(directory) = env::var_os("RECONCILE_BENCH_OUTPUT") else {
        return;
    };
    for value in [target, workload, arm] {
        assert!(value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.=".contains(&b)));
    }
    let filename = format!("{target}-{workload}-{seed}-{arm}.trace.json");
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory).expect("create result directory");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(filename))
        .expect("create new repair trace file");
    write_repair_trace(file, trace).expect("write repair trace");
}

pub fn write_case_from_env(case: Case<'_>, arms: Vec<Arm>) {
    let Some(directory) = env::var_os("RECONCILE_BENCH_OUTPUT") else {
        return;
    };
    let revision =
        env::var("RECONCILE_BENCH_REVISION").expect("declare the producing Git revision");
    let resource =
        env::var("RECONCILE_BENCH_RESOURCE_PROFILE").expect("declare host/build profile");
    assert!(!resource.trim().is_empty());
    assert!(revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()));
    let filename = format!("{}-{}-{}.json", case.target, case.workload, case.seed);
    assert!(filename
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_.=".contains(&b)));
    let report = case.report(&revision, &resource, arms);
    validate_experiment_report(&report).expect("invalid benchmark report");
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory).expect("create result directory");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(filename))
        .expect("create new result file");
    write_experiment_report(file, &report).expect("write benchmark report");
}

#[cfg(test)]
mod tests;
