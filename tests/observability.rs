// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Smoke tests for the observability instrumentation.
//!
//! These tests deliberately exercise only the **synchronous, same-thread** paths:
//! `tracing::subscriber::set_default` and `metrics::with_local_recorder` are both
//! thread-local, so events emitted on `tokio::spawn`-ed tasks (the `run()` loop) are out of
//! scope here. Running on a `current_thread` runtime keeps the `async` work on the test thread
//! so the lifecycle events of `ReplicatedMap::new` are captured.

use std::sync::{Arc, Mutex};

use reconcile::{
    replicated_map::Config, ClusterKey, InMemoryNetwork, ReadReplicaMap, ReplicatedMap,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

/// A fresh, real bindable port per call — `Config::port` must be nonzero — so the several
/// tests in this file calling `local_config()` never collide with each other. `cargo nextest`
/// runs each `#[test]` in its own process, so a process-local counter restarts at the same value
/// in every one of them — probing the OS for a genuinely free port is what stays collision-free
/// across process boundaries, not just across threads.
fn next_test_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("OS should hand out an ephemeral port")
        .local_addr()
        .expect("a bound socket reports its own address")
        .port()
}

fn local_config() -> Config {
    Config::default()
        .with_port(next_test_port())
        .with_listen_addr("127.0.0.1".parse().unwrap())
        .with_net("127.0.0.1/8".parse().unwrap())
        .unwrap()
        .with_insecure_no_key()
}

/// Keep every `tracing` callsite hot for the whole binary.
///
/// Callsite interest is cached **globally** while these tests install thread-local subscribers, so
/// a store built under `NoSubscriber` would register `Interest::never` and silence that callsite
/// for every other test.
fn keep_callsites_hot() {
    use std::sync::Once;
    use tracing_subscriber::filter::LevelFilter;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ =
            tracing::subscriber::set_global_default(Registry::default().with(LevelFilter::TRACE));
    });
}

/// A minimal `tracing` layer that records `(level, message)` for every event into a shared Vec.
#[derive(Clone, Default)]
struct CapturingLayer(Arc<Mutex<Vec<(Level, String)>>>);

struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: Subscriber> Layer<S> for CapturingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), visitor.0));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn startup_emits_info_and_security_warning_without_cluster_key() {
    keep_callsites_hot();
    let layer = CapturingLayer::default();
    let events = layer.0.clone();
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let _store = ReplicatedMap::<String, String>::new(local_config())
        .await
        .expect("bind failed");

    let events = events.lock().unwrap();
    let has_info_listening = events
        .iter()
        .any(|(level, msg)| *level == Level::INFO && msg.contains("Listening on"));
    let has_security_warn = events.iter().any(|(level, _)| *level == Level::WARN);

    assert!(
        has_info_listening,
        "expected an INFO 'Listening on' lifecycle event, captured: {events:?}"
    );
    assert!(
        has_security_warn,
        "expected a WARN security event (no cluster key set), captured: {events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_key_suppresses_the_security_warning() {
    keep_callsites_hot();
    let layer = CapturingLayer::default();
    let events = layer.0.clone();
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let config = local_config().with_cluster_key(ClusterKey::new([7u8; 32]));
    let _store = ReplicatedMap::<String, String>::new(config)
        .await
        .expect("bind failed");

    let events = events.lock().unwrap();
    let has_info_listening = events
        .iter()
        .any(|(level, msg)| *level == Level::INFO && msg.contains("Listening on"));
    let has_warn = events.iter().any(|(level, _)| *level == Level::WARN);

    // Same code path, different level outcome — proves level discrimination is wired correctly.
    assert!(
        has_info_listening,
        "expected the INFO lifecycle event regardless of authentication, captured: {events:?}"
    );
    assert!(
        !has_warn,
        "a cluster key is set, so no security WARN should fire, captured: {events:?}"
    );
}

#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn local_mutations_increment_metric_counters() {
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    // `new` emits no metrics, so build outside the recorder scope.
    keep_callsites_hot();
    let store = ReplicatedMap::<i32, i32>::new(local_config())
        .await
        .expect("bind failed");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    // The counters are recorded synchronously, so all of them land inside the recorder scope.
    with_local_recorder(&recorder, || {
        store.insert(1, 10);
        store.insert(2, 20);
        store.remove(&1);
    });

    let mut inserts = 0u64;
    let mut removes = 0u64;
    for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        if let DebugValue::Counter(v) = value {
            match composite.key().name() {
                "reconcile_inserts_total" => inserts = v,
                "reconcile_removes_total" => removes = v,
                _ => {}
            }
        }
    }

    assert_eq!(inserts, 2, "expected two inserts to be counted");
    assert_eq!(removes, 1, "expected one removal to be counted");
}

/// #83: `insert` at an exhausted write-broadcast egress budget must still apply the write, and
/// must count the skipped broadcast — `try_claim_broadcast_slot`/`record_broadcast_backpressure`
/// both run synchronously inside `insert` (before any `.await` point), so a zero budget makes the
/// skip, and its count, deterministic here.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn broadcast_backpressure_increments_the_counter_at_a_zero_budget() {
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    keep_callsites_hot();
    let store = ReplicatedMap::<i32, i32>::new(local_config().with_max_concurrent_broadcasts(0))
        .await
        .expect("bind failed");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    with_local_recorder(&recorder, || {
        assert_eq!(
            store.insert(1, 10),
            None,
            "the write itself must still apply"
        );
    });

    let mut backpressure = 0u64;
    for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        if let DebugValue::Counter(v) = value {
            if composite.key().name() == "reconcile_broadcast_backpressure_total" {
                backpressure += v;
            }
        }
    }

    assert_eq!(
        backpressure, 1,
        "expected one broadcast-backpressure skip to be counted"
    );
}

/// #27: a failed snapshot write increments `reconcile_persistence_failures_total` and raises
/// `reconcile_persistence_failures_current`; a subsequent success resets the gauge to `0`.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn persistence_failures_recorded_as_counter_and_gauge_resets_on_success() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use reconcile::persistence::{PersistedState, Persistence};

    struct FailUntilFlagged(AtomicBool);
    impl Persistence<i32, i32> for FailUntilFlagged {
        fn load(&self) -> std::io::Result<Option<PersistedState<i32, i32>>> {
            Ok(None)
        }
        fn save(&self, _state: &PersistedState<i32, i32>) -> std::io::Result<()> {
            if self.0.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(std::io::Error::other("simulated persistence failure"))
            }
        }
    }

    keep_callsites_hot();
    let backend = Arc::new(FailUntilFlagged(AtomicBool::new(false)));
    let store = ReplicatedMap::<i32, i32>::new(local_config())
        .await
        .expect("bind failed")
        .with_persistence(backend.clone());

    fn read_gauge(snapshotter: &metrics_util::debugging::Snapshotter, name: &str) -> Option<f64> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(composite, _unit, _desc, value)| {
                (composite.key().name() == name).then_some(value)
            })
            .and_then(|value| match value {
                DebugValue::Gauge(v) => Some(*v),
                _ => None,
            })
    }

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut failures_current_after_first_failure = None;
    with_local_recorder(&recorder, || {
        assert!(store.snapshot_now().is_err(), "backend starts failing");
        failures_current_after_first_failure =
            read_gauge(&snapshotter, "reconcile_persistence_failures_current");
        backend.0.store(true, Ordering::Relaxed);
        assert!(store.snapshot_now().is_ok(), "backend now succeeds");
    });

    let mut failures_total = 0u64;
    for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        if let ("reconcile_persistence_failures_total", DebugValue::Counter(v)) =
            (composite.key().name(), value)
        {
            failures_total = v;
        }
    }

    assert_eq!(failures_total, 1, "expected exactly one recorded failure");
    // Checked before the success below, or the reset there would mask a wrong (but still
    // eventually-zeroed) intermediate value.
    assert_eq!(
        failures_current_after_first_failure,
        Some(1.0),
        "the gauge must read 1 after exactly one consecutive failure"
    );
    assert_eq!(
        read_gauge(&snapshotter, "reconcile_persistence_failures_current"),
        Some(0.0),
        "the gauge must reset to 0 after the subsequent success"
    );
}

/// #27: a reconciliation round refreshes the "state right now" gauges — `reconcile_entries_current`
/// (live only) and `reconcile_tombstones_current` here, since local `insert`/`remove` calls give
/// direct control over both without needing a peer.
///
/// Uses `metrics::set_default_local_recorder` (an RAII guard), not `with_local_recorder` (a
/// synchronous closure) as the other tests in this file do: `start_reconciliation` is `async`, and
/// only the guard form can stay set across its `.await` points.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn reconciliation_round_refreshes_the_state_gauges() {
    use metrics::set_default_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    keep_callsites_hot();
    let store = ReplicatedMap::<i32, i32>::new(local_config())
        .await
        .expect("bind failed");
    store.insert(1, 10); // stays live
    store.insert(2, 20);
    store.remove(&2); // becomes a tombstone

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = set_default_local_recorder(&recorder);
    store.start_reconciliation().await;
    drop(guard);

    let mut entries_current = None;
    let mut tombstones_current = None;
    for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        match (composite.key().name(), value) {
            ("reconcile_entries_current", DebugValue::Gauge(v)) => entries_current = Some(*v),
            ("reconcile_tombstones_current", DebugValue::Gauge(v)) => {
                tombstones_current = Some(*v);
            }
            _ => {}
        }
    }

    assert_eq!(
        entries_current,
        Some(1.0),
        "one live entry (key 1) after tombstoning key 2"
    );
    assert_eq!(tombstones_current, Some(1.0), "one tombstone (key 2)");
}

/// #294: a `Config` field a `ReadReplicaMap` cannot act on (it mints no timestamps and runs no
/// bulk-transfer machinery) must not silently do nothing — a WARN naming the field is the only
/// observable trace `warn_on_ignored_config_fields` leaves, so assert on it directly rather than
/// only on the function having run.
#[tokio::test(flavor = "current_thread")]
async fn read_replica_warns_about_config_fields_it_cannot_honour() {
    keep_callsites_hot();
    let layer = CapturingLayer::default();
    let events = layer.0.clone();
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let network = InMemoryNetwork::new();
    let transport = std::sync::Arc::new(network.bind("127.0.0.1:1".parse().unwrap()));
    let config = local_config().with_remote_fanout(9);
    let _replica = ReadReplicaMap::<String, String>::new_with_transport(config, transport);

    let events = events.lock().unwrap();
    let ignored_warning = events.iter().find(|(level, msg)| {
        *level == Level::WARN && msg.contains("ReadReplicaMap ignores these Config fields")
    });
    assert!(
        ignored_warning.is_some_and(|(_, msg)| msg.contains("remote_fanout")),
        "expected a WARN naming remote_fanout as ignored, captured: {events:?}"
    );
}

/// #83: `max_concurrent_broadcasts` is another field a `ReadReplicaMap` cannot act on (it never
/// originates a local write, so there is no egress path for the field to bound) — the warning
/// above only exercises `remote_fanout` as its one representative field, so this checks the
/// `max_concurrent_broadcasts` branch of the same `if field != default` chain directly.
#[tokio::test(flavor = "current_thread")]
async fn read_replica_warns_about_max_concurrent_broadcasts_specifically() {
    keep_callsites_hot();
    let layer = CapturingLayer::default();
    let events = layer.0.clone();
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let network = InMemoryNetwork::new();
    let transport = std::sync::Arc::new(network.bind("127.0.0.1:5".parse().unwrap()));
    let config = local_config().with_max_concurrent_broadcasts(4);
    let _replica = ReadReplicaMap::<String, String>::new_with_transport(config, transport);

    let events = events.lock().unwrap();
    let ignored_warning = events.iter().find(|(level, msg)| {
        *level == Level::WARN && msg.contains("ReadReplicaMap ignores these Config fields")
    });
    assert!(
        ignored_warning.is_some_and(|(_, msg)| msg.contains("max_concurrent_broadcasts")),
        "expected a WARN naming max_concurrent_broadcasts as ignored, captured: {events:?}"
    );
}

/// The converse of the above: a `Config` that never sets a field `ReadReplicaMap` ignores must
/// not fire that WARN at all — otherwise the warning is noise, not a signal.
#[tokio::test(flavor = "current_thread")]
async fn read_replica_stays_quiet_when_no_ignored_field_is_set() {
    keep_callsites_hot();
    let layer = CapturingLayer::default();
    let events = layer.0.clone();
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let network = InMemoryNetwork::new();
    let transport = std::sync::Arc::new(network.bind("127.0.0.1:2".parse().unwrap()));
    let _replica = ReadReplicaMap::<String, String>::new_with_transport(local_config(), transport);

    let events = events.lock().unwrap();
    let has_ignored_warning = events.iter().any(|(level, msg)| {
        *level == Level::WARN && msg.contains("ReadReplicaMap ignores these Config fields")
    });
    assert!(
        !has_ignored_warning,
        "no non-default ignored field was set, so no such WARN should fire, captured: {events:?}"
    );
}

/// #294: `warn_on_ignored_config_fields` warns about `nets` specifically when more than one
/// network is declared (a `ReadReplicaMap` only ever tracks one) — the `local_config()`-based
/// tests above all use exactly one net, which cannot distinguish "more than one" from "fewer than
/// one", so this asserts the boundary directly on both sides.
#[tokio::test(flavor = "current_thread")]
async fn read_replica_warns_about_more_than_one_net_but_not_zero_or_one() {
    keep_callsites_hot();

    let nets_warning_fires = |config: Config, port: &str| {
        let layer = CapturingLayer::default();
        let events = layer.0.clone();
        let subscriber = Registry::default().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        let network = InMemoryNetwork::new();
        let transport = std::sync::Arc::new(network.bind(port.parse().unwrap()));
        let _replica = ReadReplicaMap::<String, String>::new_with_transport(config, transport);

        let events = events.lock().unwrap();
        events.iter().any(|(level, msg)| {
            *level == Level::WARN
                && msg.contains("ReadReplicaMap ignores these Config fields")
                && msg.contains("nets")
        })
    };

    let zero_nets = Config::default()
        .with_port(next_test_port())
        .with_listen_addr("127.0.0.1".parse().unwrap())
        .with_insecure_no_key();
    assert!(
        !nets_warning_fires(zero_nets, "127.0.0.1:3"),
        "zero declared networks should not warn about nets"
    );

    let two_nets = local_config()
        .with_nets(&[
            "127.0.0.1/8".parse().unwrap(),
            "10.0.0.0/8".parse().unwrap(),
        ])
        .unwrap();
    assert!(
        nets_warning_fires(two_nets, "127.0.0.1:4"),
        "more than one declared network should warn about nets"
    );
}
