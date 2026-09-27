use std::io::Write;

use super::*;

/// Renders validated observations without ranking incompatible or missing measurements.
pub fn write_experiment_summary(
    mut output: impl Write,
    report: &ExperimentReport,
) -> Result<(), ExperimentArtifactError> {
    validate_experiment_report(report)?;
    writeln!(output, "Experiment: {}", report.experiment.id)?;
    writeln!(
        output,
        "Completion: {:?}",
        report.experiment.completion_boundary
    )?;
    writeln!(
        output,
        "Resources: {}",
        report.experiment.resource_profile_id
    )?;
    writeln!(
        output,
        "Transport: {}",
        report.experiment.transport_profile_id
    )?;
    for state in &report.prepared_states {
        writeln!(
            output,
            "Prepared state: {} ({})",
            state.id, state.architecture_id
        )?;
        write_costs(&mut output, &state.build_costs)?;
    }
    for run in &report.observations {
        writeln!(
            output,
            "\n{} trial={} seed={} {:?}",
            run.architecture_id, run.trial, run.seed, run.status
        )?;
        writeln!(
            output,
            "  end-to-end: {}",
            measurement(&run.end_to_end_elapsed)
        )?;
        writeln!(
            output,
            "  prepared references: {:?}",
            run.prepared_state_refs
        )?;
        write_costs(&mut output, &run.costs)?;
    }
    Ok(())
}

fn write_costs(output: &mut impl Write, costs: &[CostRecord]) -> std::io::Result<()> {
    for cost in costs {
        for metric in &cost.metrics {
            writeln!(
                output,
                "  {:?}/{:?} {:?} peer={:?}: {}",
                cost.phase,
                cost.owner,
                metric.kind,
                metric.peer,
                measurement(&metric.measurement)
            )?;
        }
    }
    Ok(())
}

fn measurement(value: &Measurement) -> String {
    match value {
        Measurement::Observed {
            value,
            samples,
            uncertainty,
        } => format!("observed {value:?}, samples={samples}, uncertainty={uncertainty:?}"),
        Measurement::Projected {
            value,
            model_id,
            inputs_id,
        } => format!("projected {value:?}, model={model_id}, inputs={inputs_id}"),
        Measurement::Missing { unit, reason } => format!("missing {unit:?}: {reason:?}"),
    }
}
