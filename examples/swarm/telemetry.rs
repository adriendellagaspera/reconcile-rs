use std::io;

#[cfg(feature = "metrics")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
#[cfg(feature = "metrics")]
use std::sync::OnceLock;

#[cfg(feature = "metrics")]
static RECORDER: OnceLock<Snapshotter> = OnceLock::new();

pub fn install() -> io::Result<()> {
    #[cfg(feature = "metrics")]
    if RECORDER.get().is_none() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().map_err(io::Error::other)?;
        RECORDER
            .set(snapshotter)
            .map_err(|_| io::Error::other("metrics already installed"))?;
    }
    Ok(())
}

// Library counters are process totals, unlike the per-cluster demo transport counters.
// Draining the recorder's histograms on each snapshot also bounds retained observations.
pub fn snapshot() -> serde_json::Value {
    #[cfg(feature = "metrics")]
    if let Some(recorder) = RECORDER.get() {
        let counters: serde_json::Map<_, _> = recorder
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                let DebugValue::Counter(count) = value else {
                    return None;
                };
                let labels = key
                    .key()
                    .labels()
                    .map(|l| format!("{}={}", l.key(), l.value()))
                    .collect::<Vec<_>>();
                let name = if labels.is_empty() {
                    key.key().name().to_string()
                } else {
                    format!("{}:{}", key.key().name(), labels.join(","))
                };
                Some((name, serde_json::json!(count)))
            })
            .collect();
        return serde_json::Value::Object(counters);
    }
    serde_json::Value::Null
}
