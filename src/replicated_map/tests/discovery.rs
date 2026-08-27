// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::discovery::{DiscoverFuture, Discovery, DiscoveryKind};
use crate::{
    replicated_map::{Config, MemberPresence, NotAuthoritative},
    ReplicatedMap,
};

use super::ephemeral_config;

/// A scriptable discovery source for the grace/decommission tests. The test thread swaps the
/// response while the discovery loop runs.
#[derive(Clone)]
struct FakeDiscovery {
    resp: Arc<Mutex<FakeResp>>,
}

#[derive(Clone)]
enum FakeResp {
    /// A successful resolution returning this peer set.
    Present(Vec<IpAddr>),
    /// A transient failure (DNS blip).
    Blip,
}

impl FakeDiscovery {
    fn new(initial: FakeResp) -> Self {
        FakeDiscovery {
            resp: Arc::new(Mutex::new(initial)),
        }
    }
    fn set(&self, resp: FakeResp) {
        *self.resp.lock().unwrap() = resp;
    }
}

impl Discovery for FakeDiscovery {
    fn discover(&self) -> DiscoverFuture<'_> {
        let resp = self.resp.lock().unwrap().clone();
        Box::pin(async move {
            match resp {
                FakeResp::Present(addrs) => Ok(addrs),
                FakeResp::Blip => Err(Box::new(std::io::Error::other("blip")) as _),
            }
        })
    }

    fn kind(&self) -> DiscoveryKind {
        DiscoveryKind::Authoritative
    }
}

/// A discovery source that never lies about its kind — used to prove `with_discovery` rejects
/// a speculative source unconditionally, not only under `debug_assertions`.
struct SpeculativeDiscovery;

impl Discovery for SpeculativeDiscovery {
    fn discover(&self) -> DiscoverFuture<'_> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn kind(&self) -> DiscoveryKind {
        DiscoveryKind::Speculative
    }
}

/// The guard must be `assert!`, not `debug_assert!` — a no-op in `--release` would let a
/// speculative source through, whose absences then wrongly decommission live members and
/// release the causal-stability GC gate.
#[tokio::test]
#[should_panic(expected = "with_discovery expects an authoritative source")]
async fn with_discovery_rejects_a_speculative_source() {
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed");
    let _ = store.with_discovery(Arc::new(SpeculativeDiscovery));
}

/// #98: `try_with_discovery` is `with_discovery`'s non-panicking twin — same rejection, returned
/// as a `NotAuthoritative` error instead of a panic.
#[tokio::test]
async fn try_with_discovery_rejects_a_speculative_source_without_panicking() {
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed");
    assert_eq!(
        store
            .try_with_discovery(Arc::new(SpeculativeDiscovery))
            .err(),
        Some(NotAuthoritative {
            kind: DiscoveryKind::Speculative
        })
    );
}

/// `NotAuthoritative`'s `Display` text is user-facing (it's what `with_discovery` panics with) —
/// assert its actual content, not merely that formatting it doesn't panic.
#[test]
fn not_authoritative_display_names_the_actual_kind() {
    assert_eq!(
        NotAuthoritative {
            kind: DiscoveryKind::Speculative
        }
        .to_string(),
        "with_discovery expects an authoritative source, got Speculative: a speculative prober \
         would be seeded as permanent known peers and its absences would wrongly decommission \
         members"
    );
}

fn discovery_config() -> Config {
    // A real, bindable loopback address (the engine binds a socket in `new`) on an ephemeral
    // port. No `with_net`, mirroring the Kubernetes setup where discovery is purely DNS-driven.
    Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.1".parse().unwrap())
        .with_insecure_no_key()
}

/// A member that vanishes from discovery for `miss_threshold` consecutive successful rounds is
/// decommissioned; the node never decommissions itself even when absent from the result.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_decommissions_vanished_member_but_not_self() {
    let own: IpAddr = "127.0.0.1".parse().unwrap();
    let member: IpAddr = "127.0.0.200".parse().unwrap();

    let fake = FakeDiscovery::new(FakeResp::Present(vec![member]));
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(20))
        .with_discovery_miss_threshold(3);

    // Seed membership as if both had been contacted via dated datagrams.
    store.engine.members.write().insert(own);
    store.engine.members.write().insert(member);

    let loop_store = store.clone();
    let handle = tokio::spawn(async move { loop_store.discover_periodically().await });

    // While the member is present in discovery it must not be decommissioned.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        store.engine.members.read().contains(&member),
        "present member was wrongly decommissioned"
    );

    // The member vanishes; after the miss threshold it is decommissioned, but self is kept.
    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !store.engine.members.read().contains(&member),
        "vanished member was not decommissioned after the grace period"
    );
    assert!(
        store.engine.members.read().contains(&own),
        "node decommissioned itself"
    );

    handle.abort();
}

/// A transient discovery failure (DNS blip) must never decommission a member, however long it
/// lasts; only a successful resolution that omits the member counts toward the grace threshold.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_blip_does_not_decommission() {
    let member: IpAddr = "127.0.0.201".parse().unwrap();

    // Report the member present once so it enters `seen_ever`, then fail forever.
    let fake = FakeDiscovery::new(FakeResp::Present(vec![member]));
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(20))
        .with_discovery_miss_threshold(3);
    store.engine.members.write().insert(member);

    let loop_store = store.clone();
    let handle = tokio::spawn(async move { loop_store.discover_periodically().await });

    // Let the member be observed present at least once, then switch to permanent blips.
    tokio::time::sleep(Duration::from_millis(60)).await;
    fake.set(FakeResp::Blip);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        store.engine.members.read().contains(&member),
        "a transient discovery failure wrongly decommissioned a member"
    );

    // Sanity: a genuine absence still decommissions, proving the mechanism is live.
    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !store.engine.members.read().contains(&member),
        "member was not decommissioned once it genuinely vanished"
    );

    handle.abort();
}

/// #27: each discovery blip increments `reconcile_discovery_failures_total`.
///
/// `#[tokio::test]` (default flavor: `current_thread`), driving `discover_periodically` directly
/// via `timeout` rather than `tokio::spawn`ing it onto another worker thread — `metrics`' local
/// recorder is thread-local, so a spawned task on a `multi_thread` runtime would record
/// invisibly to this scope (see `tests/observability.rs`'s file-level doc comment on the same
/// constraint). `set_default_local_recorder` (an RAII guard), not `with_local_recorder` (a
/// synchronous closure): `discover_periodically` is `async`, and only the guard form can stay set
/// across its `.await` points.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn discovery_failure_increments_the_failure_counter() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let fake = FakeDiscovery::new(FakeResp::Blip);
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake))
        .with_discovery_interval(Duration::from_millis(5));

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = ::metrics::set_default_local_recorder(&recorder);
    let _ = tokio::time::timeout(Duration::from_millis(40), store.discover_periodically()).await;
    drop(guard);

    let mut failures = 0u64;
    for (composite, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        if let ("reconcile_discovery_failures_total", DebugValue::Counter(v)) =
            (composite.key().name(), value)
        {
            failures = v;
        }
    }
    assert!(
        failures >= 1,
        "expected at least one recorded discovery failure, got {failures}"
    );
}

/// #27: a run of transient discovery failures must never advance `last_successful_discovery_at`
/// (`SyncState`'s field for a caller's own readiness/alerting), and a subsequent success must.
#[tokio::test(flavor = "multi_thread")]
async fn last_successful_discovery_at_advances_only_on_success() {
    let fake = FakeDiscovery::new(FakeResp::Blip);
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(20));

    let loop_store = store.clone();
    let handle = tokio::spawn(async move { loop_store.discover_periodically().await });

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        store.sync_state().last_successful_discovery_at.is_none(),
        "a run of blips must never set last_successful_discovery_at"
    );

    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        store.sync_state().last_successful_discovery_at.is_some(),
        "a successful resolution must set last_successful_discovery_at"
    );

    handle.abort();
}

#[test]
fn member_presence_starts_present_and_requires_the_miss_threshold() {
    let mut state = MemberPresence::default();
    assert!(!state.eligible_for_decommission(1, Duration::ZERO, false));
    state.mark_missed();
    assert!(state.eligible_for_decommission(1, Duration::ZERO, false));
}

#[test]
fn member_presence_reappearance_resets_the_absence_clock_and_counter() {
    let mut state = MemberPresence::default();
    state.mark_missed();
    state.mark_missed();
    state.mark_seen();
    // A single miss after reappearing must not already clear a 2-miss threshold.
    state.mark_missed();
    assert!(!state.eligible_for_decommission(2, Duration::ZERO, false));
}

#[test]
fn member_presence_pending_tombstone_acks_require_the_wall_time_floor() {
    let mut state = MemberPresence::default();
    state.mark_missed();
    state.mark_missed();
    // Below miss_threshold: never eligible, regardless of pending acks or floor.
    assert!(!state.eligible_for_decommission(3, Duration::ZERO, true));

    state.mark_missed();
    // At the threshold, pending acks, and a floor nowhere near elapsed: held back.
    assert!(!state.eligible_for_decommission(3, Duration::from_secs(3600), true));
    // Same absence with no pending acks: the fast path is unaffected by the floor.
    assert!(state.eligible_for_decommission(3, Duration::from_secs(3600), false));
    // A zero floor is cleared instantly even with pending acks.
    assert!(state.eligible_for_decommission(3, Duration::ZERO, true));
}

/// A member with an unacknowledged tombstone survives past `miss_threshold` and is
/// decommissioned only once its absence clears the wall-time floor
/// (`ARCHITECTURE.md` §5 invariant 6).
#[tokio::test(flavor = "multi_thread")]
async fn pending_tombstone_acks_hold_decommission_past_the_miss_threshold() {
    let member: IpAddr = "127.0.0.210".parse().unwrap();

    let fake = FakeDiscovery::new(FakeResp::Present(vec![member]));
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(15))
        .with_discovery_miss_threshold(2)
        .with_discovery_decommission_floor(Duration::from_millis(300));
    store.engine.members.write().insert(member);

    // A local tombstone this member has never acknowledged.
    store.just_insert(1, 11);
    store.just_remove(&1);

    let loop_store = store.clone();
    let handle = tokio::spawn(async move { loop_store.discover_periodically().await });

    // Let the member be observed present at least once, so it is registered as discovered by
    // this source (and thus eligible for grace-decommissioning) before it vanishes.
    tokio::time::sleep(Duration::from_millis(60)).await;

    // The member vanishes. It must clear the miss threshold quickly but stay a member — the
    // floor has not elapsed yet.
    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        store.engine.members.read().contains(&member),
        "member with a pending tombstone ack was decommissioned before the wall-time floor \
         elapsed"
    );

    // Once the floor elapses, decommissioning proceeds.
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !store.engine.members.read().contains(&member),
        "member was never decommissioned even after the wall-time floor elapsed"
    );

    handle.abort();
}

/// Reappearing before the floor elapses resets the absence clock: a brief DNS blip followed by
/// recovery must never decommission the member, however close to the floor the first absence
/// got.
#[tokio::test(flavor = "multi_thread")]
async fn reappearance_resets_the_floor_for_a_member_with_pending_acks() {
    let member: IpAddr = "127.0.0.211".parse().unwrap();

    let fake = FakeDiscovery::new(FakeResp::Present(vec![member]));
    let store = ReplicatedMap::<i32, i32>::new(discovery_config())
        .await
        .expect("bind failed")
        .with_discovery(Arc::new(fake.clone()))
        .with_discovery_interval(Duration::from_millis(15))
        .with_discovery_miss_threshold(2)
        .with_discovery_decommission_floor(Duration::from_millis(300));
    store.engine.members.write().insert(member);
    store.just_insert(1, 11);
    store.just_remove(&1);

    let loop_store = store.clone();
    let handle = tokio::spawn(async move { loop_store.discover_periodically().await });

    // Absent for a while (well past miss_threshold, short of the floor), then returns.
    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(200)).await;
    fake.set(FakeResp::Present(vec![member]));
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Vanishes again; if the clock had not reset, the combined absence would already exceed
    // the floor.
    fake.set(FakeResp::Present(vec![]));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        store.engine.members.read().contains(&member),
        "reappearance did not reset the wall-time floor's absence clock"
    );

    handle.abort();
}

/// The grace-account loop's `member == own_addr || current.contains(&member)` guard must skip
/// *any* currently-present member, not just this node's own address — a `&&` there would mark a
/// live, continuously-discovered peer missed on every round and eventually decommission it.
#[tokio::test]
async fn a_continuously_present_member_is_never_decommissioned() {
    use crate::discovery::{DiscoverFuture, Discovery, DiscoveryKind};

    #[derive(Clone)]
    struct AlwaysPresent(IpAddr);
    impl Discovery for AlwaysPresent {
        fn discover(&self) -> DiscoverFuture<'_> {
            let addr = self.0;
            Box::pin(async move { Ok(vec![addr]) })
        }
        fn kind(&self) -> DiscoveryKind {
            DiscoveryKind::Authoritative
        }
    }

    let peer: IpAddr = "127.0.0.201".parse().unwrap();
    let store = ReplicatedMap::<i32, i32>::new(
        ephemeral_config().with_listen_addr("127.0.0.200".parse().unwrap()),
    )
    .await
    .expect("bind failed")
    .with_discovery_interval(Duration::from_millis(5))
    // As strict as possible: a single erroneous miss would trip this.
    .with_discovery_miss_threshold(1)
    .with_discovery(Arc::new(AlwaysPresent(peer)));
    // Seed the peer as a known member directly, bypassing a real handshake, so it appears in
    // `members_snapshot()` from round one.
    store.engine.members.write().insert(peer);

    let _ = tokio::time::timeout(Duration::from_millis(80), store.discover_periodically()).await;

    assert!(
        store.engine.members.read().contains(&peer),
        "a continuously-present member must never be decommissioned"
    );
}
