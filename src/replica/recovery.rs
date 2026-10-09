// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Capability discovery and bounded sender-side selective-recovery state.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::replicated_map::FramingConfig;

/// Opaque reserved-message payload advertising selective recovery version 1.
///
/// Keeping this inside the already-reserved length-prefixed message tag makes the extension
/// additive for peers on the same wire version: older peers decode and ignore it.
pub(crate) const SELECTIVE_RECOVERY_CAPABILITY: &[u8] = b"sr\x01";

#[derive(Clone)]
struct OutboundTransfer {
    payload: Arc<[u8]>,
    last_activity: Instant,
    last_full_send: Instant,
    recovery_rounds: u32,
}

/// Sender/receiver bookkeeping for capability-gated selective fragment recovery.
///
/// Receiver missing ranges themselves are derived from the existing bounded `Reassembler`; this
/// structure only retains sender payloads that a capable peer may request again.
#[derive(Default)]
pub(crate) struct RecoveryBook {
    capabilities: HashMap<IpAddr, Instant>,
    outbound: HashMap<(IpAddr, [u8; 32]), OutboundTransfer>,
    total_bytes: usize,
}

/// Result of retaining one outbound transfer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RetainReport {
    pub(crate) retained: bool,
    /// An identical retained transfer need not be sent in full on this retry.
    pub(crate) skip_full_send: bool,
    /// A bounded timeout permits a full-message fallback when selective control is lost.
    pub(crate) fallback_full_retry: bool,
    pub(crate) expired: usize,
    pub(crate) evicted: usize,
    pub(crate) transfers: usize,
    pub(crate) bytes: usize,
}

/// Result of expiring sender-side state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExpireReport {
    pub(crate) transfers: usize,
    pub(crate) bytes: usize,
    pub(crate) remaining_transfers: usize,
    pub(crate) remaining_bytes: usize,
}

/// A retained payload authorized for one more selective retransmission round.
pub(crate) struct RecoveryPayload {
    pub(crate) payload: Arc<[u8]>,
    pub(crate) exhausted_after_this_round: bool,
}

impl RecoveryBook {
    pub(crate) fn record_capability(
        &mut self,
        peer: IpAddr,
        now: Instant,
        cfg: FramingConfig,
        max_peers: usize,
    ) {
        if !cfg.selective_recovery {
            return;
        }
        self.expire(now, cfg);
        if self.capabilities.contains_key(&peer) || self.capabilities.len() < max_peers {
            self.capabilities.insert(peer, now);
        }
    }

    pub(crate) fn supports(&mut self, peer: IpAddr, now: Instant, cfg: FramingConfig) -> bool {
        if !cfg.selective_recovery {
            return false;
        }
        self.expire_capabilities(now, cfg.recovery_capability_ttl);
        self.capabilities.contains_key(&peer)
    }

    pub(crate) fn retain(
        &mut self,
        peer: IpAddr,
        transfer_id: [u8; 32],
        payload: &[u8],
        now: Instant,
        cfg: FramingConfig,
    ) -> RetainReport {
        let expired = self.expire_outbound(now, cfg.recovery_ttl);
        if !cfg.selective_recovery
            || payload.len() > cfg.max_outbound_recovery_bytes_per_peer
            || payload.len() > cfg.max_total_outbound_recovery_bytes
            || cfg.max_outbound_recovery_transfers_per_peer == 0
            || cfg.max_total_outbound_recovery_transfers == 0
        {
            return self.retain_report(false, expired, 0, false, false);
        }

        let key = (peer, transfer_id);
        if let Some(existing) = self.outbound.get_mut(&key) {
            // No immediate whole-message retry while NACKs can recover missing fragments.
            // The next anti-entropy send may retry the full message after 12 seconds
            // even if every selective control was lost.
            const FULL_RETRY_INTERVAL: Duration = Duration::from_secs(12);
            let fallback_full_retry =
                now.saturating_duration_since(existing.last_full_send) >= FULL_RETRY_INTERVAL;
            existing.last_activity = now;
            if fallback_full_retry {
                existing.last_full_send = now;
            }
            return self.retain_report(true, expired, 0, !fallback_full_retry, fallback_full_retry);
        }

        let mut evicted = 0;
        while self.peer_transfer_count(peer) >= cfg.max_outbound_recovery_transfers_per_peer {
            let Some(oldest) = self.oldest(Some(peer)) else {
                return self.retain_report(false, expired, evicted, false, false);
            };
            self.remove(oldest);
            evicted += 1;
        }
        while self.outbound.len() >= cfg.max_total_outbound_recovery_transfers {
            let Some(oldest) = self.oldest(None) else {
                return self.retain_report(false, expired, evicted, false, false);
            };
            self.remove(oldest);
            evicted += 1;
        }
        while self.peer_bytes(peer).saturating_add(payload.len())
            > cfg.max_outbound_recovery_bytes_per_peer
        {
            let Some(oldest) = self.oldest(Some(peer)) else {
                return self.retain_report(false, expired, evicted, false, false);
            };
            self.remove(oldest);
            evicted += 1;
        }
        while self.total_bytes.saturating_add(payload.len()) > cfg.max_total_outbound_recovery_bytes
        {
            let Some(oldest) = self.oldest(None) else {
                return self.retain_report(false, expired, evicted, false, false);
            };
            self.remove(oldest);
            evicted += 1;
        }

        self.total_bytes += payload.len();
        self.outbound.insert(
            key,
            OutboundTransfer {
                payload: Arc::from(payload),
                last_activity: now,
                last_full_send: now,
                recovery_rounds: 0,
            },
        );
        self.retain_report(true, expired, evicted, false, false)
    }

    pub(crate) fn payload_for_report(
        &mut self,
        peer: IpAddr,
        transfer_id: [u8; 32],
        now: Instant,
        cfg: FramingConfig,
    ) -> Option<RecoveryPayload> {
        self.expire_outbound(now, cfg.recovery_ttl);
        let transfer = self.outbound.get_mut(&(peer, transfer_id))?;
        if transfer.recovery_rounds >= cfg.max_selective_recovery_rounds {
            return None;
        }
        transfer.recovery_rounds += 1;
        transfer.last_activity = now;
        Some(RecoveryPayload {
            payload: Arc::clone(&transfer.payload),
            exhausted_after_this_round: transfer.recovery_rounds
                >= cfg.max_selective_recovery_rounds,
        })
    }

    pub(crate) fn complete(&mut self, peer: IpAddr, transfer_id: [u8; 32]) -> bool {
        self.remove((peer, transfer_id)).is_some()
    }

    pub(crate) fn forget(&mut self, peer: IpAddr, transfer_id: [u8; 32]) -> bool {
        self.remove((peer, transfer_id)).is_some()
    }

    pub(crate) fn expire(&mut self, now: Instant, cfg: FramingConfig) -> ExpireReport {
        self.expire_capabilities(now, cfg.recovery_capability_ttl);
        let before_bytes = self.total_bytes;
        let expired = self.expire_outbound(now, cfg.recovery_ttl);
        ExpireReport {
            transfers: expired,
            bytes: before_bytes.saturating_sub(self.total_bytes),
            remaining_transfers: self.outbound.len(),
            remaining_bytes: self.total_bytes,
        }
    }

    pub(crate) fn occupancy(&self) -> (usize, usize) {
        (self.outbound.len(), self.total_bytes)
    }

    fn expire_capabilities(&mut self, now: Instant, ttl: Duration) {
        self.capabilities
            .retain(|_, seen| now.saturating_duration_since(*seen) < ttl);
    }

    fn expire_outbound(&mut self, now: Instant, ttl: Duration) -> usize {
        let expired: Vec<_> = self
            .outbound
            .iter()
            .filter_map(|(&key, transfer)| {
                (now.saturating_duration_since(transfer.last_activity) >= ttl).then_some(key)
            })
            .collect();
        let count = expired.len();
        for key in expired {
            self.remove(key);
        }
        count
    }

    fn peer_transfer_count(&self, peer: IpAddr) -> usize {
        self.outbound.keys().filter(|(p, _)| *p == peer).count()
    }

    fn peer_bytes(&self, peer: IpAddr) -> usize {
        self.outbound
            .iter()
            .filter(|((p, _), _)| *p == peer)
            .map(|(_, transfer)| transfer.payload.len())
            .sum()
    }

    fn oldest(&self, peer: Option<IpAddr>) -> Option<(IpAddr, [u8; 32])> {
        self.outbound
            .iter()
            .filter(|((p, _), _)| peer.is_none_or(|wanted| *p == wanted))
            .min_by_key(|((p, id), transfer)| (transfer.last_activity, *p, *id))
            .map(|(&key, _)| key)
    }

    fn remove(&mut self, key: (IpAddr, [u8; 32])) -> Option<OutboundTransfer> {
        let removed = self.outbound.remove(&key)?;
        self.total_bytes = self.total_bytes.saturating_sub(removed.payload.len());
        Some(removed)
    }

    fn retain_report(
        &self,
        retained: bool,
        expired: usize,
        evicted: usize,
        skip_full_send: bool,
        fallback_full_retry: bool,
    ) -> RetainReport {
        RetainReport {
            retained,
            skip_full_send,
            fallback_full_retry,
            expired,
            evicted,
            transfers: self.outbound.len(),
            bytes: self.total_bytes,
        }
    }
}

/// Whether one reserved extension payload advertises selective recovery.
pub(crate) fn is_selective_recovery_capability(payload: &[u8]) -> bool {
    payload == SELECTIVE_RECOVERY_CAPABILITY
}

#[cfg(test)]
mod tests;
