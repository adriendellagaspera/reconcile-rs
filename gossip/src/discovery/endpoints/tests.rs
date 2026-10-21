// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

struct Hints(Vec<String>);
impl EndpointDiscovery<String> for Hints {
    fn discover(&self, _limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, String> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

struct Failure;
impl EndpointDiscovery<String> for Failure {
    fn discover(&self, _limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, String> {
        Box::pin(async { Err(io::Error::other("resolver unavailable").into()) })
    }
}

struct NeverResolves;
impl EndpointDiscovery<String> for NeverResolves {
    fn discover(&self, _limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, String> {
        Box::pin(std::future::pending())
    }
}

fn config(candidates: usize, probes: usize) -> EndpointDiscoveryConfig {
    EndpointDiscoveryConfig::new(
        NonZeroUsize::new(candidates).unwrap(),
        NonZeroUsize::new(probes).unwrap(),
        Duration::from_millis(5),
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
fn invalid_limits_are_rejected_by_the_configuration() {
    let one = NonZeroUsize::new(1).unwrap();
    let two = NonZeroUsize::new(2).unwrap();
    for (candidates, probes, timeout, interval) in [
        (one, two, Duration::from_secs(1), Duration::from_secs(1)),
        (one, one, Duration::ZERO, Duration::from_secs(1)),
        (one, one, Duration::from_secs(1), Duration::ZERO),
    ] {
        assert_eq!(
            EndpointDiscoveryConfig::new(candidates, probes, timeout, interval)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(EndpointDiscoveryConfig::new(
        one,
        one,
        Duration::from_nanos(1),
        Duration::from_nanos(1)
    )
    .is_ok());
}

#[tokio::test]
async fn bounded_deduplicated_rounds_rotate_without_starvation() {
    // All budgets, including equal-to-cap, exercise the same scheduling properties.
    for candidates in 1..=8 {
        for budget in 1..=candidates {
            let mut state = EndpointDiscoveryState::new(config(candidates * 2, budget));
            let names: Vec<_> = (0..candidates).map(|n| format!("optical-{n}")).collect();
            let source = Hints(names.iter().chain(&names).cloned().collect());
            state.refresh(&source).await.unwrap();
            assert_eq!(state.candidate_count(), candidates);
            let now = Instant::now();
            let mut observed = Vec::new();
            for round in 0..candidates {
                // Repeated identical snapshots must not restart rotation or cooldown.
                state.refresh(&source).await.unwrap();
                let tick = now + Duration::from_secs(round as u64);
                let batch = state.next_probes(tick);
                assert_eq!(batch.len(), budget);
                assert!(batch
                    .iter()
                    .enumerate()
                    .all(|(i, item)| !batch[..i].contains(item)));
                observed.extend(batch);
                assert!(state.next_probes(tick).is_empty());
                assert!(state
                    .next_probes(tick + Duration::from_millis(999))
                    .is_empty());
                assert!(state.next_probes(now - Duration::from_secs(1)).is_empty());
            }
            assert!(names.iter().all(|name| observed.contains(name)));
        }
    }
}

#[tokio::test]
async fn refresh_keeps_the_next_endpoint_when_order_changes() {
    let mut state = EndpointDiscoveryState::new(config(3, 1));
    state
        .refresh(&Hints(vec!["a".into(), "b".into(), "c".into()]))
        .await
        .unwrap();
    let now = Instant::now();
    assert_eq!(state.next_probes(now), ["a"]);
    state
        .refresh(&Hints(vec!["c".into(), "b".into(), "a".into()]))
        .await
        .unwrap();
    assert_eq!(state.next_probes(now + Duration::from_secs(1)), ["b"]);
}

#[tokio::test]
async fn errors_oversized_results_and_timeout_keep_prior_hints_and_cooldown() {
    let mut state = EndpointDiscoveryState::new(config(2, 1));
    state
        .refresh(&Hints(vec!["a".into(), "b".into()]))
        .await
        .unwrap();
    let now = Instant::now();
    assert_eq!(state.next_probes(now), ["a"]);
    assert!(state.refresh(&Failure).await.is_err());
    assert_eq!(
        state
            .refresh(&Hints(vec!["x".into(); 3]))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        state.refresh(&NeverResolves).await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(state.candidate_count(), 2);
    assert!(state.next_probes(now).is_empty());
    assert_eq!(state.next_probes(now + Duration::from_secs(1)), ["b"]);
}

#[tokio::test]
async fn empty_refresh_and_cancellation_cannot_reset_probe_cooldown() {
    let mut state = EndpointDiscoveryState::new(
        EndpointDiscoveryConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(3600),
            Duration::from_secs(1),
        )
        .unwrap(),
    );
    state.refresh(&Hints(vec!["a".into()])).await.unwrap();
    let now = Instant::now();
    assert_eq!(state.next_probes(now), ["a"]);
    let mut pending = Box::pin(state.refresh(&NeverResolves));
    std::future::poll_fn(|context| {
        assert!(pending.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(pending);
    assert_eq!(state.candidate_count(), 1);
    state.refresh(&Hints(vec![])).await.unwrap();
    assert_eq!(state.candidate_count(), 0);
    assert!(state.next_probes(now + Duration::from_secs(1)).is_empty());
    state.refresh(&Hints(vec!["b".into()])).await.unwrap();
    assert!(state.next_probes(now).is_empty());
    assert_eq!(state.next_probes(now + Duration::from_secs(1)), ["b"]);
}

#[tokio::test]
async fn consumer_requests_its_configured_raw_cap_through_a_trait_object() {
    struct Source;
    impl EndpointDiscovery<String> for Source {
        fn discover(&self, limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, String> {
            Box::pin(async move { Ok((0..limit.get()).map(|n| n.to_string()).collect()) })
        }
    }
    let mut state = EndpointDiscoveryState::new(config(7, 7));
    let source: &dyn EndpointDiscovery<String> = &Source;
    state.refresh(source).await.unwrap();
    assert_eq!(state.candidate_count(), 7);
    assert_eq!(state.next_probes(Instant::now()).len(), 7);
}

#[tokio::test]
async fn a_batch_never_repeats_endpoints_when_budget_exceeds_the_hint_set() {
    let mut state = EndpointDiscoveryState::new(config(4, 4));
    state
        .refresh(&Hints(vec!["a".into(), "a".into(), "b".into()]))
        .await
        .unwrap();
    let now = Instant::now();
    assert_eq!(state.next_probes(now), ["a", "b"]);
    state
        .refresh(&Hints(vec!["replacement".into()]))
        .await
        .unwrap();
    assert_eq!(
        state.next_probes(now + Duration::from_secs(1)),
        ["replacement"]
    );
}

#[tokio::test]
async fn default_limits_retain_256_candidates_and_probe_16_per_second() {
    struct Source;
    impl EndpointDiscovery<String> for Source {
        fn discover(&self, limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, String> {
            assert_eq!(limit.get(), 256);
            Box::pin(async move { Ok((0..limit.get()).map(|n| n.to_string()).collect()) })
        }
    }
    let mut state = EndpointDiscoveryState::new(EndpointDiscoveryConfig::default());
    state.refresh(&Source).await.unwrap();
    assert_eq!(state.candidate_count(), 256);
    let now = Instant::now();
    assert_eq!(state.next_probes(now).len(), 16);
    assert!(state
        .next_probes(now + Duration::from_millis(999))
        .is_empty());
    assert_eq!(state.next_probes(now + Duration::from_secs(1)).len(), 16);
}
