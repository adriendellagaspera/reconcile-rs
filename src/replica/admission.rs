// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cmp::max;
use std::collections::HashMap;
use std::net::IpAddr;
use web_time::{Duration, Instant};

use parking_lot::Mutex;

/// Smallest burst that preserves the wide-scattered single-round convergence regression. The
/// steady per-source rate is one newly selected workset per second after this initial allowance.
pub(super) const PER_PEER_BULK_BURST: u32 = 3;
const PER_PEER_REFILL_INTERVAL: Duration = Duration::from_secs(1);
const REPEATED_WORK_RETRY_MIN: Duration = Duration::from_secs(1);
const REPEATED_WORK_RETRY_MAX: Duration = Duration::from_secs(30);
const RECENT_WORK_RETENTION: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BulkAdmissionRejection {
    RepeatedWork,
    PeerRate,
    GlobalRate,
}

#[derive(Clone, Copy)]
struct RecentWork {
    fingerprint: [u8; 32],
    retry_after: Instant,
    retry_delay: Duration,
    forget_after: Instant,
}

#[derive(Default)]
struct AdmissionState {
    /// GCRA theoretical-arrival time per source IP. Entries with no remaining rate debt are
    /// discarded, so idle authenticated senders do not accumulate here forever.
    peers: HashMap<IpAddr, Instant>,
    global_tat: Option<Instant>,
    recent_dated: HashMap<IpAddr, RecentWork>,
    recent_value: HashMap<IpAddr, RecentWork>,
}

/// Admission for newly selected dataset-scale bulk work.
///
/// Repeating the same peer comparison without evidence of progress is suppressed with an
/// exponential retry delay (1 s, 2 s, 4 s, ... capped at 30 s). A changed comparison resets that
/// delay immediately. GCRA separately bounds genuinely different comparisons: burst three then one
/// per second per source, and globally three times the concurrent-dump budget then that budget per
/// second. Active dump slots and byte pacing remain separate bounds.
pub(super) struct BulkAdmission {
    state: Mutex<AdmissionState>,
    global_rate: usize,
    global_interval: Duration,
    global_burst: u32,
}

impl BulkAdmission {
    pub(super) fn new(global_rate: usize) -> Self {
        let global_interval = if global_rate == 0 {
            Duration::from_secs(1)
        } else {
            Duration::from_secs_f64(1.0 / global_rate as f64)
        };
        let global_burst = global_rate
            .saturating_mul(PER_PEER_BULK_BURST as usize)
            .min(u32::MAX as usize) as u32;

        Self {
            state: Mutex::new(AdmissionState::default()),
            global_rate,
            global_interval,
            global_burst,
        }
    }

    pub(super) fn try_admit_at(
        &self,
        peer: IpAddr,
        channel: super::pacing::DumpChannel,
        fingerprint: [u8; 32],
        now: Instant,
    ) -> Result<(), BulkAdmissionRejection> {
        if self.global_rate == 0 {
            return Err(BulkAdmissionRejection::GlobalRate);
        }

        let mut state = self.state.lock();
        state.peers.retain(|_, tat| *tat > now);
        state.recent_dated.retain(|_, work| work.forget_after > now);
        state.recent_value.retain(|_, work| work.forget_after > now);
        if state.global_tat.is_some_and(|tat| tat <= now) {
            state.global_tat = None;
        }

        let recent = match channel {
            super::pacing::DumpChannel::Dated => &mut state.recent_dated,
            super::pacing::DumpChannel::ValueOnly => &mut state.recent_value,
        };
        if recent
            .get(&peer)
            .is_some_and(|work| work.fingerprint == fingerprint && work.retry_after > now)
        {
            return Err(BulkAdmissionRejection::RepeatedWork);
        }

        let peer_tat = state.peers.get(&peer).copied().unwrap_or(now);
        if !gcra_allows(peer_tat, now, PER_PEER_REFILL_INTERVAL, PER_PEER_BULK_BURST) {
            return Err(BulkAdmissionRejection::PeerRate);
        }

        let global_tat = state.global_tat.unwrap_or(now);
        if !gcra_allows(global_tat, now, self.global_interval, self.global_burst) {
            return Err(BulkAdmissionRejection::GlobalRate);
        }

        state
            .peers
            .insert(peer, max(peer_tat, now) + PER_PEER_REFILL_INTERVAL);
        state.global_tat = Some(max(global_tat, now) + self.global_interval);
        let recent = match channel {
            super::pacing::DumpChannel::Dated => &mut state.recent_dated,
            super::pacing::DumpChannel::ValueOnly => &mut state.recent_value,
        };
        let retry_delay = recent
            .get(&peer)
            .filter(|work| work.fingerprint == fingerprint)
            .map(|work| std::cmp::min(work.retry_delay.saturating_mul(2), REPEATED_WORK_RETRY_MAX))
            .unwrap_or(REPEATED_WORK_RETRY_MIN);
        recent.insert(
            peer,
            RecentWork {
                fingerprint,
                retry_after: now + retry_delay,
                retry_delay,
                forget_after: now + RECENT_WORK_RETENTION,
            },
        );
        Ok(())
    }
}

/// Generic Cell Rate Algorithm admission: burst arrivals may happen immediately, then arrivals
/// replenish every interval. This is token-bucket-equivalent without fractional token state.
fn gcra_allows(tat: Instant, now: Instant, interval: Duration, burst: u32) -> bool {
    let allowance = interval.saturating_mul(burst.saturating_sub(1));
    now.checked_add(allowance).is_none_or(|limit| tat <= limit)
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use web_time::{Duration, Instant};

    use super::{BulkAdmission, BulkAdmissionRejection, PER_PEER_BULK_BURST};
    use crate::replica::pacing::DumpChannel;

    fn ip(last: u8) -> IpAddr {
        format!("127.0.0.{last}").parse().unwrap()
    }

    fn fp(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn identical_work_is_suppressed_and_does_not_spend_a_token() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        assert_eq!(
            admission.try_admit_at(ip(1), DumpChannel::Dated, fp(1), now),
            Ok(())
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(1),
                now + Duration::from_millis(100)
            ),
            Err(BulkAdmissionRejection::RepeatedWork)
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(2),
                now + Duration::from_millis(100)
            ),
            Ok(())
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(3),
                now + Duration::from_millis(100)
            ),
            Ok(())
        );
    }

    #[test]
    fn repeated_work_retries_back_off_without_rejection_extending_the_deadline() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        admission
            .try_admit_at(ip(1), DumpChannel::Dated, fp(1), now)
            .unwrap();
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(1),
                now + Duration::from_millis(900)
            ),
            Err(BulkAdmissionRejection::RepeatedWork)
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(1),
                now + Duration::from_secs(1)
            ),
            Ok(())
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(1),
                now + Duration::from_millis(2500)
            ),
            Err(BulkAdmissionRejection::RepeatedWork)
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(1),
                now + Duration::from_millis(3200)
            ),
            Ok(())
        );
    }

    #[test]
    fn peer_rate_debt_expires_at_the_refill_deadline() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        admission
            .try_admit_at(ip(1), DumpChannel::Dated, fp(1), now)
            .unwrap();

        admission
            .try_admit_at(
                ip(2),
                DumpChannel::Dated,
                fp(2),
                now + Duration::from_secs(1),
            )
            .unwrap();

        let state = admission.state.lock();
        assert!(!state.peers.contains_key(&ip(1)));
    }

    #[test]
    fn dated_recent_work_expires_at_the_retention_deadline() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        admission
            .try_admit_at(ip(1), DumpChannel::Dated, fp(1), now)
            .unwrap();

        admission
            .try_admit_at(
                ip(2),
                DumpChannel::ValueOnly,
                fp(2),
                now + Duration::from_secs(60),
            )
            .unwrap();

        let state = admission.state.lock();
        assert!(!state.recent_dated.contains_key(&ip(1)));
    }

    #[test]
    fn expired_value_only_recent_work_is_reclaimed() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        admission
            .try_admit_at(ip(1), DumpChannel::ValueOnly, fp(1), now)
            .unwrap();

        admission
            .try_admit_at(
                ip(2),
                DumpChannel::Dated,
                fp(2),
                now + Duration::from_secs(61),
            )
            .unwrap();

        let state = admission.state.lock();
        assert!(!state.recent_value.contains_key(&ip(1)));
    }

    #[test]
    fn value_only_recent_work_expires_at_the_retention_deadline() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        admission
            .try_admit_at(ip(1), DumpChannel::ValueOnly, fp(1), now)
            .unwrap();

        admission
            .try_admit_at(
                ip(2),
                DumpChannel::Dated,
                fp(2),
                now + Duration::from_secs(60),
            )
            .unwrap();

        let state = admission.state.lock();
        assert!(!state.recent_value.contains_key(&ip(1)));
    }

    #[test]
    fn same_work_on_other_channel_is_distinct() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        assert_eq!(
            admission.try_admit_at(ip(1), DumpChannel::Dated, fp(1), now),
            Ok(())
        );
        assert_eq!(
            admission.try_admit_at(ip(1), DumpChannel::ValueOnly, fp(1), now),
            Ok(())
        );
    }

    #[test]
    fn per_peer_burst_is_finite_then_refills_one_per_second() {
        let admission = BulkAdmission::new(4);
        let now = Instant::now();
        for i in 0..PER_PEER_BULK_BURST {
            assert_eq!(
                admission.try_admit_at(ip(1), DumpChannel::Dated, fp(i as u8), now),
                Ok(())
            );
        }
        assert_eq!(
            admission.try_admit_at(ip(1), DumpChannel::Dated, fp(99), now),
            Err(BulkAdmissionRejection::PeerRate)
        );
        assert_eq!(
            admission.try_admit_at(
                ip(1),
                DumpChannel::Dated,
                fp(99),
                now + Duration::from_secs(1)
            ),
            Ok(())
        );
    }

    #[test]
    fn global_burst_and_refill_scale_with_concurrent_dump_budget() {
        let admission = BulkAdmission::new(2);
        let now = Instant::now();
        let global_burst = 2 * PER_PEER_BULK_BURST as usize;
        for i in 0..global_burst {
            assert_eq!(
                admission.try_admit_at(ip((i + 1) as u8), DumpChannel::Dated, fp(i as u8), now),
                Ok(())
            );
        }
        assert_eq!(
            admission.try_admit_at(ip(200), DumpChannel::Dated, fp(99), now),
            Err(BulkAdmissionRejection::GlobalRate)
        );
        assert_eq!(
            admission.try_admit_at(
                ip(200),
                DumpChannel::Dated,
                fp(99),
                now + Duration::from_millis(500)
            ),
            Ok(())
        );
    }
}
