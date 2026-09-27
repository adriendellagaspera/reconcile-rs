use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::metrics::{CostRecord, Measurement};

pub const EXPERIMENT_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentRevision {
    pub name: String,
    pub version: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompletionBoundary {
    DifferenceDiscovered,
    IdsResolved,
    PayloadTransferred,
    RepairApplied,
    LogicalViewVerified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ViewSemantics {
    Frozen,
    Snapshot,
    Cutoff,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalViewSpec {
    pub id: String,
    pub semantics: ViewSemantics,
    pub cutoff: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSpec {
    pub schema_version: u32,
    pub id: String,
    pub workload_id: String,
    pub logical_task_id: String,
    pub logical_view: LogicalViewSpec,
    pub completion_boundary: CompletionBoundary,
    pub resource_profile_id: String,
    pub transport_profile_id: String,
}

impl ExperimentSpec {
    pub(super) fn same_comparison_task(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    Scan,
    RangeScan,
    Lookup,
    ChangeFeed,
    Summary,
    ConsistentView,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityState {
    Native,
    Addon,
    Unsupported,
    Forbidden,
}

impl CapabilityState {
    pub(super) fn available(self) -> bool {
        matches!(self, Self::Native | Self::Addon)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectureSpec {
    pub id: String,
    pub backend: ComponentRevision,
    pub addon: Option<ComponentRevision>,
    pub protocol: ComponentRevision,
    pub required_capabilities: Vec<Capability>,
    pub capabilities: BTreeMap<Capability, CapabilityState>,
}

impl ArchitectureSpec {
    pub(super) fn first_unavailable_capability(&self) -> Option<Capability> {
        self.required_capabilities
            .iter()
            .copied()
            .find(|capability| {
                !self
                    .capabilities
                    .get(capability)
                    .copied()
                    .unwrap_or(CapabilityState::Unsupported)
                    .available()
            })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LifecyclePhase {
    BaseFixtureProvisioning,
    InitialArchitectureBuild,
    SessionPreparation,
    SessionWork,
    IncrementalMaintenance,
    RecoveryOrRebuild,
    Verification,
    Teardown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CostOwner {
    BaseStore,
    Addon,
    Protocol,
    Transport,
    Verifier,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedState {
    pub id: String,
    pub architecture_id: String,
    pub logical_view_id: String,
    pub build_costs: Vec<CostRecord>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Completed,
    Disabled,
    Failed,
    TimedOut,
    Censored,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunObservation {
    pub experiment_id: String,
    pub architecture_id: String,
    pub trial: u32,
    pub seed: u64,
    pub actual_logical_view_id: String,
    pub prepared_state_refs: Vec<String>,
    pub costs: Vec<CostRecord>,
    pub end_to_end_elapsed: Measurement,
    pub status: RunStatus,
}
