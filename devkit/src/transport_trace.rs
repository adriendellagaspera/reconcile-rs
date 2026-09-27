use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Direction {
    AToB,
    BToA,
}

impl Direction {
    pub fn reverse(self) -> Self {
        match self {
            Self::AToB => Self::BToA,
            Self::BToA => Self::AToB,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageKind {
    Refinement,
    Enumeration,
    MerkleRequest,
    MerkleHashes,
    RowFetch,
    EqualityDigest,
    CodedSymbols,
    StopAck,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceMessage {
    pub direction: Direction,
    pub kind: MessageKind,
    pub payload_bytes: usize,
    pub streamable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceStage {
    /// Messages in one stage may be framed independently, but the next stage cannot begin until
    /// this stage's dependency boundary has been satisfied.
    pub messages: Vec<TraceMessage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolTrace {
    pub protocol: String,
    pub stages: Vec<TraceStage>,
}

impl ProtocolTrace {
    pub fn total_payload_bytes(&self) -> usize {
        self.stages
            .iter()
            .flat_map(|stage| &stage.messages)
            .map(|message| message.payload_bytes)
            .sum()
    }

    pub fn message_count(&self) -> usize {
        self.stages
            .iter()
            .map(|stage| stage.messages.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_totals_payload_and_messages() {
        let trace = ProtocolTrace {
            protocol: "test".to_owned(),
            stages: vec![
                TraceStage {
                    messages: vec![TraceMessage {
                        direction: Direction::AToB,
                        kind: MessageKind::Refinement,
                        payload_bytes: 10,
                        streamable: false,
                    }],
                },
                TraceStage {
                    messages: vec![
                        TraceMessage {
                            direction: Direction::BToA,
                            kind: MessageKind::Enumeration,
                            payload_bytes: 20,
                            streamable: false,
                        },
                        TraceMessage {
                            direction: Direction::BToA,
                            kind: MessageKind::RowFetch,
                            payload_bytes: 30,
                            streamable: false,
                        },
                    ],
                },
            ],
        };

        assert_eq!(trace.total_payload_bytes(), 60);
        assert_eq!(trace.message_count(), 3);
    }

    #[test]
    fn direction_reverse_is_involution() {
        assert_eq!(Direction::AToB.reverse().reverse(), Direction::AToB);
        assert_eq!(Direction::BToA.reverse().reverse(), Direction::BToA);
    }

    #[test]
    fn trace_round_trips_through_json() {
        let trace = ProtocolTrace {
            protocol: "riblt".to_owned(),
            stages: vec![TraceStage {
                messages: vec![TraceMessage {
                    direction: Direction::AToB,
                    kind: MessageKind::CodedSymbols,
                    payload_bytes: 24,
                    streamable: true,
                }],
            }],
        };
        let json = serde_json::to_string(&trace).unwrap();
        assert_eq!(serde_json::from_str::<ProtocolTrace>(&json).unwrap(), trace);
    }
}
