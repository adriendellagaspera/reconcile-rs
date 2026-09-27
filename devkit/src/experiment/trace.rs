use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

pub const REPAIR_TRACE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepairStrategy {
    Rbsr,
    Merkle,
    Riblt,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RepairStage {
    /// The refinement payload travels from the opposite side to `responder`; any enumerated
    /// payload travels back from `responder` to the opposite side. The following RBSR round,
    /// when present, carries the responder's refined child ranges in that same return direction.
    RbsrRound {
        responder: PeerSide,
        refinement_ranges: u64,
        refinement_bytes: u64,
        enumeration_ranges: u64,
        enumerated_elements: u64,
        enumerated_bytes: Vec<u64>,
        frameable_outputs: u64,
    },
    /// Left is the requester and Right the responder: request bytes travel Left -> Right and
    /// response hashes travel Right -> Left.
    MerkleExchange {
        depth: u32,
        request_prefixes: u64,
        request_bytes: u64,
        response_hashes: u64,
        response_bytes: u64,
    },
    /// Left requests final keys from Right; returned rows travel Right -> Left.
    MerkleFetch {
        request_keys: u64,
        request_bytes: u64,
        returned_rows: u64,
        response_bytes: u64,
    },
    /// Left sends its equality digest to Right. If it differs, Right can immediately begin the
    /// coded-symbol stream after receiving this stage.
    RibltEquality { bytes: u64 },
    /// Right continuously sends coded symbols to Left until Left can decode.
    RibltStream {
        coded_symbols: u64,
        coded_symbol_bytes: u64,
    },
    /// After decoding, Left signals Right to stop the rateless stream.
    RibltStopAck { bytes: u64 },
}

impl RepairStage {
    pub fn is_streamable(&self) -> bool {
        matches!(self, Self::RibltStream { .. })
    }

    pub fn independently_frameable_outputs(&self) -> u64 {
        match self {
            Self::RbsrRound {
                frameable_outputs, ..
            } => *frameable_outputs,
            Self::MerkleExchange {
                request_prefixes,
                response_hashes,
                ..
            } => request_prefixes + response_hashes,
            Self::MerkleFetch {
                request_keys,
                returned_rows,
                ..
            } => request_keys + returned_rows,
            Self::RibltEquality { .. } | Self::RibltStopAck { .. } => 1,
            Self::RibltStream { coded_symbols, .. } => *coded_symbols,
        }
    }

    pub fn byte_variants(&self) -> Vec<u64> {
        match self {
            Self::RbsrRound {
                refinement_bytes,
                enumerated_bytes,
                ..
            } => {
                if enumerated_bytes.is_empty() {
                    vec![*refinement_bytes]
                } else {
                    enumerated_bytes
                        .iter()
                        .map(|payload| refinement_bytes + payload)
                        .collect()
                }
            }
            Self::MerkleExchange {
                request_bytes,
                response_bytes,
                ..
            }
            | Self::MerkleFetch {
                request_bytes,
                response_bytes,
                ..
            } => vec![request_bytes + response_bytes],
            Self::RibltEquality { bytes } | Self::RibltStopAck { bytes } => vec![*bytes],
            Self::RibltStream {
                coded_symbols,
                coded_symbol_bytes,
            } => vec![coded_symbols * coded_symbol_bytes],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepairTrace {
    pub schema_version: u32,
    pub strategy: RepairStrategy,
    pub stages: Vec<RepairStage>,
}

impl RepairTrace {
    pub fn new(strategy: RepairStrategy, stages: Vec<RepairStage>) -> Self {
        Self {
            schema_version: REPAIR_TRACE_SCHEMA_VERSION,
            strategy,
            stages,
        }
    }

    pub fn total_byte_variants(&self) -> Vec<u64> {
        let variants = self
            .stages
            .iter()
            .map(RepairStage::byte_variants)
            .map(|bytes| bytes.len())
            .max()
            .unwrap_or(1);
        let mut totals = vec![0; variants];

        for stage in &self.stages {
            let bytes = stage.byte_variants();
            if bytes.len() == 1 && variants > 1 {
                for total in &mut totals {
                    *total += bytes[0];
                }
            } else {
                assert_eq!(
                    bytes.len(),
                    variants,
                    "trace stages must use one shared payload-variant cardinality"
                );
                for (total, bytes) in totals.iter_mut().zip(bytes) {
                    *total += bytes;
                }
            }
        }
        totals
    }

    pub fn dependency_stages(&self) -> usize {
        self.stages.len()
    }
}

pub fn write_repair_trace(
    mut writer: impl Write,
    trace: &RepairTrace,
) -> Result<(), serde_json::Error> {
    serde_json::to_writer_pretty(&mut writer, trace)?;
    Ok(())
}

pub fn read_repair_trace(reader: impl Read) -> Result<RepairTrace, serde_json::Error> {
    serde_json::from_reader(reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_broadcast_single_variant_across_payload_variants() {
        let trace = RepairTrace::new(
            RepairStrategy::Rbsr,
            vec![
                RepairStage::RbsrRound {
                    responder: PeerSide::Right,
                    refinement_ranges: 1,
                    refinement_bytes: 10,
                    enumeration_ranges: 1,
                    enumerated_elements: 1,
                    enumerated_bytes: vec![20, 30],
                    frameable_outputs: 2,
                },
                RepairStage::RbsrRound {
                    responder: PeerSide::Left,
                    refinement_ranges: 1,
                    refinement_bytes: 5,
                    enumeration_ranges: 0,
                    enumerated_elements: 0,
                    enumerated_bytes: vec![],
                    frameable_outputs: 1,
                },
            ],
        );
        assert_eq!(trace.total_byte_variants(), vec![35, 45]);
    }

    #[test]
    fn typed_stages_expose_streaming_and_frameability() {
        let stream = RepairStage::RibltStream {
            coded_symbols: 7,
            coded_symbol_bytes: 24,
        };
        assert!(stream.is_streamable());
        assert_eq!(stream.independently_frameable_outputs(), 7);
        assert_eq!(stream.byte_variants(), vec![168]);
    }

    #[test]
    fn json_round_trip_is_lossless() {
        let trace = RepairTrace::new(
            RepairStrategy::Merkle,
            vec![RepairStage::MerkleExchange {
                depth: 3,
                request_prefixes: 4,
                request_bytes: 36,
                response_hashes: 64,
                response_bytes: 2_048,
            }],
        );
        let mut bytes = Vec::new();
        write_repair_trace(&mut bytes, &trace).unwrap();
        assert_eq!(read_repair_trace(bytes.as_slice()).unwrap(), trace);
    }
}
