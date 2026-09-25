// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Optional Prometheus integration, enabled by the `metrics-prometheus` feature.
//! The library emits through the [`metrics`] facade and never installs a recorder itself. These
//! helpers install one and either serve `/metrics` or hand back the exposition text.

use std::error::Error as StdError;
use std::fmt;
use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;

/// Why installing the Prometheus recorder failed. Opaque wrapper over the
/// `metrics-exporter-prometheus` crate's own error: a public signature naming it directly
/// would force every dependent onto this crate's exact exporter version for a type they only ever
/// propagate, never match on. The underlying error is reachable through
/// [`std::error::Error::source`].
#[derive(Debug)]
pub struct PrometheusError(metrics_exporter_prometheus::BuildError);

impl fmt::Display for PrometheusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to install the Prometheus recorder: {}", self.0)
    }
}

impl StdError for PrometheusError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.0)
    }
}

impl From<metrics_exporter_prometheus::BuildError> for PrometheusError {
    fn from(err: metrics_exporter_prometheus::BuildError) -> Self {
        PrometheusError(err)
    }
}

/// A handle to the installed Prometheus recorder, returned by [`install_recorder`].
/// Opaque wrapper over the exporter's own handle type, for the same reason as
/// [`PrometheusError`].
pub struct PrometheusHandle(metrics_exporter_prometheus::PrometheusHandle);

impl PrometheusHandle {
    /// Render the current metrics in Prometheus exposition format.
    #[must_use]
    pub fn render(&self) -> String {
        self.0.render()
    }
}

/// Install a global Prometheus recorder with no HTTP server; [`PrometheusHandle::render`] gives
/// the `/metrics` body.
/// # Errors
/// If a recorder is already installed — call this exactly once, early in `main`.
pub fn install_recorder() -> Result<PrometheusHandle, PrometheusError> {
    let handle = PrometheusBuilder::new().install_recorder()?;
    crate::observability::describe();
    Ok(PrometheusHandle(handle))
}

/// Install the recorder and spawn a background HTTP server exposing `/metrics` at `addr`.
/// Requires a Tokio runtime, and returns once the listener is up. Call exactly once.
pub async fn serve(addr: SocketAddr) -> Result<(), PrometheusError> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;
    crate::observability::describe();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single self-contained test: the Prometheus recorder is process-global (`metrics`'s
    /// facade allows exactly one), so this covers `render`'s actual content and `PrometheusError`'s
    /// `Display`/`source` together rather than across separate tests that could race on which
    /// installs first under a threaded (non-nextest) runner.
    #[test]
    fn render_reports_recorded_metrics_and_a_second_install_errors_with_a_source() {
        let handle = install_recorder().expect("first install should succeed");
        ::metrics::counter!("reconcile_prometheus_test_total").increment(1);
        let body = handle.render();
        assert!(
            body.contains("reconcile_prometheus_test_total"),
            "expected the recorded metric's name in the rendered body: {body}"
        );
        // `install_recorder` calls `observability::describe`: a mutant collapsing that
        // call to a no-op would still pass the assertion above (a bare, undescribed counter
        // renders fine) but drop every `# HELP` line — assert on one directly. The exporter only
        // emits a `# HELP` line for a metric that also has a recorded sample, hence the increment.
        ::metrics::counter!(crate::metrics::PERSISTENCE_FAILURES_TOTAL).increment(1);
        let body = handle.render();
        assert!(
            body.contains(
                "# HELP reconcile_persistence_failures_total Snapshot writes to the \
                 persistence backend that failed"
            ),
            "expected describe() to have registered a HELP line for \
             reconcile_persistence_failures_total: {body}"
        );

        let err = match install_recorder() {
            Ok(_) => panic!("a second global recorder install should fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("failed to install the Prometheus recorder"),
            "unexpected Display: {err}"
        );
        assert!(
            StdError::source(&err).is_some(),
            "expected source() to chain to the underlying BuildError"
        );
    }
}
