// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Selective-recovery control and retransmission sending.

use std::net::SocketAddr;
use web_time::{Duration, Instant};

use parking_lot::Mutex;

use serde::Serialize;

use crate::observability;
use crate::replicated_map::FramingConfig;
use crate::transport::Transport;
use gossip::framing;

use super::framed_send::{send_frame_to_retry, SendPorts};
use super::{Message, SELECTIVE_RECOVERY_CAPABILITY};

pub(crate) fn append_capability<K, V, P>(framing: FramingConfig, out: &mut Vec<u8>)
where
    K: Serialize,
    V: Serialize,
    P: Serialize,
{
    if framing.selective_recovery {
        gossip::bincode::encode(
            &Message::<K, V, P>::Reserved6(SELECTIVE_RECOVERY_CAPABILITY.to_vec()),
            out,
        )
        .expect("serializing the fixed recovery capability cannot fail");
    }
}

/// Send one already-encoded authenticated selective-recovery control frame.
pub(crate) async fn send_recovery_control_to<T: Transport + ?Sized>(
    ports: &SendPorts<'_, T>,
    peer: SocketAddr,
    frame: &[u8],
    missing_ranges: Option<usize>,
) -> std::io::Result<usize> {
    let sent = send_frame_to_retry(
        ports.transport,
        ports.authenticator,
        ports.sender_counter,
        frame,
        peer,
    )
    .await?;
    match missing_ranges {
        Some(ranges) => observability::record_selective_recovery_request(ranges, sent),
        None => observability::record_completion_ack(sent),
    }
    Ok(sent)
}

/// Retransmit only authenticated missing byte ranges for a retained transfer.
///
/// False means the report is stale, invalid, unsupported, or its bounded recovery-round budget is
/// exhausted. Normal anti-entropy remains the convergence fallback in every such case.
pub(crate) async fn retransmit_missing_to<T: Transport + ?Sized>(
    ports: &SendPorts<'_, T>,
    peer: SocketAddr,
    transfer_id: [u8; 32],
    ranges: &[(u32, u32)],
) -> bool {
    if !ports.framing.selective_recovery {
        observability::record_selective_recovery_fallback("disabled");
        return false;
    }
    let now = Instant::now();
    let recovery_payload = {
        let mut recovery = ports.recovery.lock();
        if !recovery.supports(peer.ip(), now, ports.framing) {
            None
        } else {
            recovery.payload_for_report(peer.ip(), transfer_id, now, ports.framing)
        }
    };
    let Some(recovery_payload) = recovery_payload else {
        observability::record_selective_recovery_fallback("stale_or_exhausted");
        return false;
    };
    let payload = recovery_payload.payload;
    let fragment_capacity = framing::fragment_payload_capacity(
        ports.framing.datagram_payload_budget,
        ports.authenticator.overhead(),
    )
    .unwrap_or(0);
    if fragment_capacity == 0 {
        observability::record_selective_recovery_fallback("invalid_capacity");
        return false;
    }

    let mut previous_end = 0usize;
    for &(offset, len) in ranges {
        let start = offset as usize;
        let Some(end) = start.checked_add(len as usize) else {
            observability::record_selective_recovery_fallback("invalid_report");
            return false;
        };
        if len == 0 || end > payload.len() || start < previous_end {
            observability::record_selective_recovery_fallback("invalid_report");
            return false;
        }
        previous_end = end;
    }

    let mut frame = Vec::new();
    for &(offset, len) in ranges {
        let start = offset as usize;
        let end = start + len as usize;
        for (relative, chunk) in payload[start..end].chunks(fragment_capacity).enumerate() {
            let chunk_offset = start + relative * fragment_capacity;
            if framing::write_fragment(transfer_id, payload.len(), chunk_offset, chunk, &mut frame)
                .is_err()
            {
                observability::record_selective_recovery_fallback("invalid_report");
                return false;
            }
            if send_frame_to_retry(
                ports.transport,
                ports.authenticator,
                ports.sender_counter,
                &frame,
                peer,
            )
            .await
            .is_err()
            {
                return false;
            }
            observability::record_fragment_sent();
            observability::record_selective_retransmit(chunk.len());
        }
    }

    if recovery_payload.exhausted_after_this_round {
        let (transfers, bytes) = {
            let mut recovery = ports.recovery.lock();
            recovery.forget(peer.ip(), transfer_id);
            recovery.occupancy()
        };
        observability::record_outbound_recovery_state(transfers, bytes);
    }
    true
}

/// Retire retained sender state after an authenticated completion acknowledgement.
pub(crate) fn complete_recovery<T: Transport + ?Sized>(
    ports: &SendPorts<'_, T>,
    peer: SocketAddr,
    transfer_id: [u8; 32],
) {
    let (changed, transfers, bytes) = {
        let mut recovery = ports.recovery.lock();
        let changed = recovery.complete(peer.ip(), transfer_id);
        let (transfers, bytes) = recovery.occupancy();
        (changed, transfers, bytes)
    };
    if changed {
        observability::record_outbound_recovery_state(transfers, bytes);
    }
}

/// Expire capability and retained sender state on the ordinary idle cadence.
pub(crate) fn expire_recovery_state<T: Transport + ?Sized>(ports: &SendPorts<'_, T>) {
    let report = ports.recovery.lock().expire(Instant::now(), ports.framing);
    observability::record_outbound_recovery_evictions("ttl", report.transfers);
    observability::record_outbound_recovery_state(
        report.remaining_transfers,
        report.remaining_bytes,
    );
}

/// Periodically request missing ranges for incomplete transfers even if the terminal fragment
/// never arrived. Control is authenticated, capability gated and bounded by the reassembler's
/// existing peer/byte limits, a per-transfer request interval and a per-tick global cap.
pub(crate) async fn retry_idle_incomplete<T: Transport + ?Sized>(
    ports: &SendPorts<'_, T>,
    reassembler: &Mutex<framing::Reassembler>,
    port: u16,
) {
    if !ports.framing.selective_recovery {
        return;
    }
    let limit = framing::max_missing_ranges_for_budget(
        ports.framing.datagram_payload_budget,
        ports.authenticator.overhead(),
        ports.framing.max_missing_ranges_per_report,
    );
    // Expiry is recorded through the shared observability gate, not silently by polling.
    crate::framing::expire_reassembly(reassembler);
    let requests = reassembler.lock().poll_idle_missing(
        Instant::now(),
        Duration::from_secs(1),
        Duration::from_secs(3),
        limit,
        32,
    );
    for request in requests {
        if !ports
            .recovery
            .lock()
            .supports(request.peer, Instant::now(), ports.framing)
        {
            observability::record_selective_recovery_fallback("unsupported_peer");
            continue;
        }
        let mut frame = Vec::new();
        if framing::write_missing_report(request.transfer_id, &request.ranges, &mut frame).is_ok() {
            let peer = SocketAddr::new(request.peer, port);
            let _ = send_recovery_control_to(ports, peer, &frame, Some(request.ranges.len())).await;
        }
    }
}

#[cfg(test)]
mod tests;
