use super::*;

use crate::metrics as names;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

fn counter(snapshot: &Snapshotter, name: &str, label: Option<(&str, &str)>) -> Option<u64> {
    snapshot
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            (key.key().name() == name
                && label.is_none_or(|(k, v)| {
                    key.key().labels().any(|l| l.key() == k && l.value() == v)
                }))
            .then_some(value)
            .and_then(|v| match v {
                DebugValue::Counter(count) => Some(count),
                _ => None,
            })
        })
}

fn gauge(snapshot: &Snapshotter, name: &str) -> Option<f64> {
    snapshot
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            (key.key().name() == name)
                .then_some(value)
                .and_then(|v| match v {
                    DebugValue::Gauge(reading) => Some(*reading),
                    _ => None,
                })
        })
}

#[test]
fn selective_metrics_capture_retransmissions_control_and_bounded_state() {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    ::metrics::with_local_recorder(&recorder, || {
        record_selective_retransmit(11);
        record_selective_retransmit(17);
        record_selective_recovery_request(3, 67);
        record_selective_recovery_request(1, 49);
        record_completion_ack(35);
        record_selective_recovery_fallback("unsupported_peer");
        record_selective_recovery_fallback("stale_or_exhausted");
        record_outbound_recovery_evictions("zero_only", 0);
        record_outbound_recovery_evictions("ttl", 2);
        record_outbound_recovery_evictions("capacity", 3);
        record_outbound_recovery_state(2, 1234);
    });
    assert_eq!(
        counter(
            &snapshot,
            names::OUTBOUND_RECOVERY_EVICTIONS_TOTAL,
            Some(("reason", "zero_only"))
        ),
        None,
        "zero evictions must not create a metric series"
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_RETRANSMITTED_FRAGMENTS_TOTAL,
            None
        ),
        Some(2)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_RETRANSMITTED_BYTES_TOTAL,
            None
        ),
        Some(28)
    );
    assert_eq!(
        counter(&snapshot, names::SELECTIVE_RECOVERY_REQUESTS_TOTAL, None),
        Some(2)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_MISSING_RANGES_TOTAL,
            None
        ),
        Some(4)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_COMPLETION_ACKS_TOTAL,
            None
        ),
        Some(1)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_CONTROL_BYTES_TOTAL,
            None
        ),
        Some(151)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_FALLBACKS_TOTAL,
            Some(("reason", "unsupported_peer"))
        ),
        Some(1)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::SELECTIVE_RECOVERY_FALLBACKS_TOTAL,
            Some(("reason", "stale_or_exhausted"))
        ),
        Some(1)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::OUTBOUND_RECOVERY_EVICTIONS_TOTAL,
            Some(("reason", "ttl"))
        ),
        Some(2)
    );
    assert_eq!(
        counter(
            &snapshot,
            names::OUTBOUND_RECOVERY_EVICTIONS_TOTAL,
            Some(("reason", "capacity"))
        ),
        Some(3)
    );
    assert_eq!(
        gauge(&snapshot, names::OUTBOUND_RECOVERY_TRANSFERS_CURRENT),
        Some(2.0)
    );
    assert_eq!(
        gauge(&snapshot, names::OUTBOUND_RECOVERY_BYTES_CURRENT),
        Some(1234.0)
    );

    ::metrics::with_local_recorder(&recorder, || {
        record_outbound_recovery_state(0, 0);
    });
    assert_eq!(
        gauge(&snapshot, names::OUTBOUND_RECOVERY_TRANSFERS_CURRENT),
        Some(0.0)
    );
    assert_eq!(
        gauge(&snapshot, names::OUTBOUND_RECOVERY_BYTES_CURRENT),
        Some(0.0)
    );
}
