use super::*;

#[test]
fn local_measurements_preserve_boundaries_and_round_trip() {
    let report = Case {
        target: "discovery",
        workload: "fixture",
        seed: 7,
        records: BTreeMap::from([("left".to_owned(), 10), ("right".to_owned(), 11)]),
        symmetric_difference: Some(1),
    }
    .report(
        "revision",
        "host-build",
        vec![Arm::new(
            "rbsr",
            "rbsr",
            "revision",
            vec![
                elapsed(
                    LifecyclePhase::SessionWork,
                    CostOwner::Protocol,
                    Duration::from_millis(3),
                ),
                protocol_bytes(64, "counted-wire", "fixture"),
            ],
        )],
    );
    let mut json = Vec::new();
    write_experiment_report(&mut json, &report).unwrap();
    assert_eq!(read_experiment_report(json.as_slice()).unwrap(), report);
    let mut human = Vec::new();
    write_experiment_summary(&mut human, &report).unwrap();
    let human = String::from_utf8(human).unwrap();
    assert!(human.contains("observed"));
    assert!(human.contains("projected"));
    assert!(human.contains("missing Seconds: NotMeasured"));
    let mut legacy = report.clone();
    legacy.schema_version = 1;
    assert!(read_experiment_report(serde_json::to_vec(&legacy).unwrap().as_slice()).is_err());
    let run = &report.observations[0];
    assert!(matches!(
        run.end_to_end_elapsed,
        Measurement::Missing { .. }
    ));
    assert!(matches!(
        run.costs[0].metrics[0].measurement,
        Measurement::Observed { .. }
    ));
    assert!(matches!(
        run.costs[1].metrics[0].measurement,
        Measurement::Projected { .. }
    ));
    assert!(
        validate_ranking_candidate(&report.experiment, &report.architectures[0], run, &[]).is_err()
    );
}
