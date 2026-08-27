// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #46: `Config::snapshot_interval` becoming `Option<Duration>` (periodic snapshotting can be
//! disabled outright) and the new `Config::snapshot_change_threshold` (a periodic wakeup only
//! writes once enough changes have accumulated, so an idle node does zero snapshot IO).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::persistence::{PersistedState, Persistence};
use crate::ReplicatedMap;

use super::ephemeral_config;

/// A [`Persistence`] backend that always succeeds and counts how many times [`save`](Persistence::save)
/// was called — lets a test assert *whether* the periodic task wrote, not just what it wrote.
struct CountingSave {
    saves: Arc<AtomicUsize>,
}

impl<K: Send + Sync + 'static, V: Send + Sync + 'static> Persistence<K, V> for CountingSave {
    fn load(&self) -> std::io::Result<Option<PersistedState<K, V>>> {
        Ok(None)
    }
    fn save(&self, _state: &PersistedState<K, V>) -> std::io::Result<()> {
        self.saves.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Fewer than [`Config::snapshot_change_threshold`](crate::replicated_map::Config::snapshot_change_threshold)
/// changes since the last snapshot: a periodic wakeup must skip the write entirely.
#[tokio::test]
async fn snapshot_periodically_skips_below_change_threshold() {
    let saves = Arc::new(AtomicUsize::new(0));
    let short_interval = Duration::from_millis(20);
    let store = ReplicatedMap::<i32, i32>::new(
        ephemeral_config()
            .with_snapshot_interval(Some(short_interval))
            .with_snapshot_change_threshold(3),
    )
    .await
    .expect("bind failed")
    .with_persistence(Arc::new(CountingSave {
        saves: saves.clone(),
    }));

    // Two changes: below the configured threshold of three.
    store.just_insert(1, 10);
    store.just_insert(2, 20);

    let store2 = store.clone();
    let periodic = tokio::spawn(async move { store2.snapshot_periodically().await });
    tokio::time::sleep(short_interval * 10).await;
    periodic.abort();

    assert_eq!(
        saves.load(Ordering::Relaxed),
        0,
        "fewer changes than snapshot_change_threshold must never trigger a write"
    );
}

/// The converse of the above: once accumulated changes reach the threshold, the next periodic
/// wakeup writes.
#[tokio::test]
async fn snapshot_periodically_writes_once_threshold_reached() {
    let saves = Arc::new(AtomicUsize::new(0));
    let short_interval = Duration::from_millis(20);
    let store = ReplicatedMap::<i32, i32>::new(
        ephemeral_config()
            .with_snapshot_interval(Some(short_interval))
            .with_snapshot_change_threshold(3),
    )
    .await
    .expect("bind failed")
    .with_persistence(Arc::new(CountingSave {
        saves: saves.clone(),
    }));

    store.just_insert(1, 10);
    store.just_insert(2, 20);
    store.just_insert(3, 30);

    let store2 = store.clone();
    let periodic = tokio::spawn(async move { store2.snapshot_periodically().await });
    tokio::time::sleep(short_interval * 10).await;
    periodic.abort();

    assert!(
        saves.load(Ordering::Relaxed) >= 1,
        "reaching snapshot_change_threshold must trigger a write on a later wakeup"
    );
}

/// A successful snapshot resets the change count: a second idle stretch (no new changes) after
/// the first write must not trigger another one, even at the default threshold of `1` — this is
/// the "idle node does zero snapshot IO" behavior the issue asks for.
#[tokio::test]
async fn snapshot_periodically_does_not_rewrite_when_idle_after_a_snapshot() {
    let saves = Arc::new(AtomicUsize::new(0));
    let short_interval = Duration::from_millis(20);
    let store = ReplicatedMap::<i32, i32>::new(
        ephemeral_config().with_snapshot_interval(Some(short_interval)),
    )
    .await
    .expect("bind failed")
    .with_persistence(Arc::new(CountingSave {
        saves: saves.clone(),
    }));

    store.just_insert(1, 10);

    let store2 = store.clone();
    let periodic = tokio::spawn(async move { store2.snapshot_periodically().await });
    // Long enough for several ticks: one write for the one change, then idle wakeups.
    tokio::time::sleep(short_interval * 10).await;
    periodic.abort();

    assert_eq!(
        saves.load(Ordering::Relaxed),
        1,
        "an idle node after its first snapshot must not be snapshotted again"
    );
}

/// `snapshot_interval: None` disables the periodic background task outright: the call must
/// return immediately rather than looping forever, or `run`'s `tokio::join!` would block on it
/// forever whenever persistence is configured with periodic snapshotting turned off.
#[tokio::test]
async fn snapshot_interval_none_disables_periodic_task() {
    let store = ReplicatedMap::<i32, i32>::new(ephemeral_config().with_snapshot_interval(None))
        .await
        .expect("bind failed");

    tokio::time::timeout(Duration::from_secs(2), store.snapshot_periodically())
        .await
        .expect("snapshot_periodically must return immediately when snapshot_interval is None");
}
