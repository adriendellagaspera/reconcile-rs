// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::Duration;

use crate::{
    replicated_map::{Config, ConfigError, ConstructionError, MAX_NETS},
    ReadReplicaMap, ReplicatedMap,
};

use super::virtual_config;

/// Construction rejects an omitted security choice as a typed configuration error before any
/// transport is bound. Full and read replicas share the same contract.
#[tokio::test]
async fn missing_security_mode_is_a_typed_construction_error() {
    let config = Config::default().with_port(5000);

    let full_error = match ReplicatedMap::<i32, i32>::new(config.clone()).await {
        Ok(_) => panic!("missing security mode must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        full_error,
        ConstructionError::Config(ConfigError::MissingSecurityMode)
    ));

    let read_error = match ReadReplicaMap::<i32, i32>::new(config).await {
        Ok(_) => panic!("missing security mode must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        read_error,
        ConstructionError::Config(ConfigError::MissingSecurityMode)
    ));
}

/// Injected transports cannot bypass the explicit security-mode requirement.
#[test]
fn injected_transport_constructors_reject_missing_security_mode() {
    use std::sync::Arc;

    use crate::transport::InMemoryNetwork;

    let network = InMemoryNetwork::new();
    let config = Config::default().with_port(8081);
    let full = ReplicatedMap::<i32, i32>::new_with_transport(
        config.clone(),
        Arc::new(network.bind("127.0.0.1:8081".parse().unwrap())),
    );
    assert!(matches!(
        full,
        Err(ConstructionError::Config(ConfigError::MissingSecurityMode))
    ));

    let read = ReadReplicaMap::<i32, i32>::new_with_transport(
        config,
        Arc::new(network.bind("127.0.0.2:8081".parse().unwrap())),
    );
    assert!(matches!(
        read,
        Err(ConstructionError::Config(ConfigError::MissingSecurityMode))
    ));
}

/// Construction errors retain their typed cause for callers traversing the standard error chain.
#[test]
fn construction_error_source_preserves_configuration_and_io_causes() {
    let configuration = ConstructionError::Config(ConfigError::MissingSecurityMode);
    let source = std::error::Error::source(&configuration).expect("configuration cause");
    assert_eq!(
        source.downcast_ref::<ConfigError>(),
        Some(&ConfigError::MissingSecurityMode)
    );

    let transport = ConstructionError::Io(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "transport binding failed",
    ));
    let source = std::error::Error::source(&transport).expect("transport cause");
    let io_error = source
        .downcast_ref::<std::io::Error>()
        .expect("typed I/O cause");
    assert_eq!(io_error.kind(), std::io::ErrorKind::AddrInUse);
    assert_eq!(io_error.to_string(), "transport binding failed");
}

/// The metering/buffer-size defaults are pinned to their documented values (32 MiB/s, 1 MiB
/// floor, 8 MiB) — an accidental `*` → `+`/`/` typo in the byte-count arithmetic would silently
/// shrink these by orders of magnitude without a type error to catch it.
#[test]
fn byte_rate_defaults_match_their_documented_values() {
    assert_eq!(
        super::super::config::DEFAULT_BULK_SEND_RATE,
        32 * 1024 * 1024
    );
    assert_eq!(super::super::MIN_BULK_SEND_RATE, 1024 * 1024);
    assert_eq!(
        super::super::config::DEFAULT_SOCKET_BUFFER_SIZE,
        8 * 1024 * 1024
    );
}

/// `ConfigError::TooManyNets`'s `Display` text is user-facing — assert its actual content, not
/// merely that formatting it doesn't panic.
#[test]
fn config_error_too_many_nets_display_names_the_limit() {
    assert_eq!(
        ConfigError::TooManyNets.to_string(),
        format!("at most {MAX_NETS} networks are supported")
    );
}

/// `with_nets` rejects a batch that would exceed [`MAX_NETS`] with a `Result`, the bulk-form
/// counterpart `with_net` already returns per-net.
#[test]
fn with_nets_rejects_a_batch_past_max_nets() {
    use ipnet::IpNet;

    let net: IpNet = "10.0.0.0/8".parse().unwrap();
    let nets = vec![net; MAX_NETS + 1];
    assert_eq!(
        Config::default().with_nets(&nets).err(),
        Some(ConfigError::TooManyNets)
    );

    // Exactly at the cap still succeeds, and sets every slot in order.
    let nets = vec![net; MAX_NETS];
    let cfg = Config::default().with_nets(&nets).unwrap();
    assert!(cfg.nets.iter().all(|slot| *slot == Some(net)));
}

/// `Config`'s hand-written `Debug` impl exists to redact `cluster_key` — assert it actually
/// hides the key material and still reports `Some`/`None` correctly either way.
#[test]
fn config_debug_redacts_cluster_key_but_not_its_presence() {
    let key_bytes = [0xABu8; 32];
    let with_key = virtual_config().with_cluster_key(gossip::auth::ClusterKey::new(key_bytes));
    let debug = format!("{with_key:?}");
    assert!(
        debug.contains("<redacted>"),
        "expected the key material to be redacted: {debug}"
    );
    assert!(
        !debug.contains("ab, ab, ab") && !debug.contains("171, 171, 171"),
        "raw key bytes must not appear in Debug output: {debug}"
    );

    let without_key = virtual_config();
    let debug = format!("{without_key:?}");
    assert!(
        debug.contains("cluster_key: None"),
        "expected an explicit None, got: {debug}"
    );
}

#[test]
fn resource_caps_default_to_documented_values() {
    let cfg = Config::default();
    assert_eq!(cfg.max_replay_senders, 1024);
    assert_eq!(cfg.max_concurrent_broadcasts, 1024);
    assert_eq!(cfg.max_concurrent_bulk_dumps, 4);
}

/// The new fields default to the documented values: [`SNAPSHOT_INTERVAL`] (5 s) and
/// [`MAX_CLOCK_DRIFT`](crate::clock::MAX_CLOCK_DRIFT) (1 h) — not e.g. `Duration::ZERO`, which
/// would make every write pay a snapshot or silently disable the drift clamp. adds
/// `snapshot_change_threshold`, defaulting to `1` — an idle node still skips the write, but any
/// single change is enough to trigger one, matching the always-write behavior.
/// [`SNAPSHOT_INTERVAL`]: super::super::persistence::SNAPSHOT_INTERVAL
#[test]
fn snapshot_interval_and_max_clock_drift_default_correctly() {
    let cfg = Config::default();
    assert_eq!(
        cfg.snapshot_interval,
        Some(super::super::persistence::SNAPSHOT_INTERVAL)
    );
    assert_eq!(cfg.snapshot_change_threshold, 1);
    assert_eq!(cfg.max_clock_drift, crate::clock::MAX_CLOCK_DRIFT);
}
