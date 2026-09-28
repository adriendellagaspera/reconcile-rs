use super::*;

#[test]
fn durations_reject_negative_observations_and_projections_but_accept_zero() {
    for value in [-1.0, 0.0, 1.0] {
        for measurement in [
            observed_seconds(value),
            Measurement::Projected {
                value: MetricValue::Seconds(value),
                model_id: "duration-model".to_owned(),
                inputs_id: "trial".to_owned(),
            },
        ] {
            let mut observation = run(RunStatus::Completed);
            observation.end_to_end_elapsed = measurement.clone();
            assert_eq!(
                validate_observation(&experiment(), &architecture(), &observation, &[]).is_ok(),
                value >= 0.0
            );
            for kind in [
                MetricKind::LocalElapsedSeconds,
                MetricKind::CpuSeconds,
                MetricKind::FreshnessLagSeconds,
            ] {
                let mut observation = run(RunStatus::Completed);
                observation.costs[0].metrics[0] = CostMetric {
                    kind,
                    peer: None,
                    measurement: measurement.clone(),
                };
                assert_eq!(
                    validate_observation(&experiment(), &architecture(), &observation, &[]).is_ok(),
                    value >= 0.0
                );
            }
        }
    }
}

#[test]
fn foreground_deltas_preserve_negative_improvements() {
    for (kind, value) in [
        (
            MetricKind::ForegroundP95DeltaSeconds,
            MetricValue::Seconds(-0.1),
        ),
        (
            MetricKind::ForegroundP99DeltaSeconds,
            MetricValue::Seconds(-0.1),
        ),
        (
            MetricKind::ForegroundThroughputDeltaRatio,
            MetricValue::Ratio(-0.1),
        ),
    ] {
        let mut observation = run(RunStatus::Completed);
        observation.costs[0].metrics[0] = CostMetric {
            kind,
            peer: None,
            measurement: Measurement::Observed {
                value,
                samples: 1,
                uncertainty: None,
            },
        };
        assert!(validate_observation(&experiment(), &architecture(), &observation, &[]).is_ok());
    }
}
