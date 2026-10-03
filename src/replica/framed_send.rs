// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::{debug, error, instrument, trace, warn};

use crate::observability;
use crate::replicated_map::FramingConfig;
use crate::transport::Transport;
use gossip::{auth, framing, replay};

use super::{Message, MAX_SENDTO_RETRIES};

/// The four things every framed send needs.
pub(crate) struct SendPorts<'a, T: ?Sized> {
    pub(crate) transport: &'a T,
    pub(crate) authenticator: &'a auth::Authenticator,
    pub(crate) sender_counter: &'a replay::SenderCounter,
    pub(crate) framing: FramingConfig,
}

struct Pacer {
    rate: Option<usize>,
    started: Instant,
    sent_bytes: usize,
}

impl Pacer {
    fn new(rate: Option<usize>) -> Self {
        Pacer {
            rate,
            started: Instant::now(),
            sent_bytes: 0,
        }
    }

    async fn before_send(&self) {
        let Some(rate) = self.rate.filter(|&rate| rate > 0) else {
            return;
        };
        if self.sent_bytes == 0 {
            return;
        }
        let expected = Duration::from_secs_f64(self.sent_bytes as f64 / rate as f64);
        if let Some(delay) = expected.checked_sub(self.started.elapsed()) {
            tokio::time::sleep(delay).await;
        }
    }

    fn record(&mut self, bytes: usize) {
        self.sent_bytes = self.sent_bytes.saturating_add(bytes);
    }
}

async fn send_frame_to_retry<T: Transport + ?Sized>(
    transport: &T,
    authenticator: &auth::Authenticator,
    sender_counter: &replay::SenderCounter,
    frame: &[u8],
    target: SocketAddr,
) -> std::io::Result<usize> {
    let seq = sender_counter.next_seq();
    let stamp = sender_counter.next_stamp();
    let wire = authenticator.seal(seq, stamp, frame);
    let mut result = Ok(0);
    for _ in 0..MAX_SENDTO_RETRIES {
        result = transport.send_to(&wire, &target).await;
        if result.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    match &result {
        Ok(sent) => observability::record_bytes_sent(*sent),
        Err(err) => {
            error!("send_to failed after {MAX_SENDTO_RETRIES} retries: {err}");
            observability::record_send_failure();
        }
    }
    result
}

fn capacities(
    authenticator: &auth::Authenticator,
    config: FramingConfig,
) -> Option<(usize, usize)> {
    Some((
        framing::complete_payload_capacity(
            config.datagram_payload_budget,
            authenticator.overhead(),
        )?,
        framing::fragment_payload_capacity(
            config.datagram_payload_budget,
            authenticator.overhead(),
        )?,
    ))
}

async fn send_logical_payload<T: Transport + ?Sized>(
    ports: &SendPorts<'_, T>,
    peer: SocketAddr,
    payload: &[u8],
    frame_buf: &mut Vec<u8>,
    pacer: &mut Pacer,
) -> std::io::Result<usize> {
    let Some((complete_capacity, fragment_capacity)) =
        capacities(ports.authenticator, ports.framing)
    else {
        observability::record_value_oversized();
        return Ok(0);
    };

    if payload.len() <= complete_capacity {
        framing::write_complete(payload, frame_buf);
        pacer.before_send().await;
        let sent = send_frame_to_retry(
            ports.transport,
            ports.authenticator,
            ports.sender_counter,
            frame_buf,
            peer,
        )
        .await?;
        pacer.record(payload.len());
        return Ok(sent);
    }

    if payload.len() > ports.framing.max_logical_message_size || fragment_capacity == 0 {
        error!(
            "dropping logical message to {peer}: encodes to {} bytes, configured maximum is {}",
            payload.len(),
            ports.framing.max_logical_message_size
        );
        observability::record_value_oversized();
        return Ok(0);
    }

    let fragment_count = payload.len().div_ceil(fragment_capacity);
    if fragment_count > ports.framing.max_fragments_per_message {
        error!(
            "dropping logical message to {peer}: requires {fragment_count} fragments, configured maximum is {}",
            ports.framing.max_fragments_per_message
        );
        observability::record_value_oversized();
        return Ok(0);
    }

    observability::record_fragmented_message();
    let id = framing::transfer_id(payload);
    let mut total_sent = 0usize;
    for (index, chunk) in payload.chunks(fragment_capacity).enumerate() {
        let offset = index * fragment_capacity;
        framing::write_fragment(id, payload.len(), offset, chunk, frame_buf)
            .expect("validated framing lengths fit the on-wire u32 fields");
        pacer.before_send().await;
        match send_frame_to_retry(
            ports.transport,
            ports.authenticator,
            ports.sender_counter,
            frame_buf,
            peer,
        )
        .await
        {
            Ok(sent) => {
                total_sent = total_sent.saturating_add(sent);
                observability::record_fragment_sent();
            }
            Err(err) => {
                warn!("failed to send fragment to {peer}: {err}; continuing");
            }
        }
        pacer.record(chunk.len());
    }
    Ok(total_sent)
}

/// Send one already-encoded logical protocol payload, fragmenting it when needed.
pub(crate) async fn send_to_retry<T: Transport + ?Sized>(
    transport: &T,
    authenticator: &auth::Authenticator,
    sender_counter: &replay::SenderCounter,
    framing: FramingConfig,
    payload: &[u8],
    target: SocketAddr,
) -> std::io::Result<usize> {
    let ports = SendPorts {
        transport,
        authenticator,
        sender_counter,
        framing,
    };
    let mut frame_buf = Vec::new();
    let mut pacer = Pacer::new(None);
    send_logical_payload(&ports, target, payload, &mut frame_buf, &mut pacer).await
}

/// Send messages back-to-back using MTU-safe application framing.
pub(crate) async fn send_messages_to<K, V, P, T>(
    messages: &[Message<K, V, P>],
    ports: &SendPorts<'_, T>,
    peer: &SocketAddr,
    send_buf: &mut Vec<u8>,
) where
    K: Serialize,
    V: Serialize,
    P: Serialize,
    T: Transport + ?Sized,
{
    send_messages_paced(messages, ports, peer, send_buf, None).await;
}

/// Pack small messages into complete frames and fragment only a single encoded message that
/// exceeds the complete-frame capacity. Each fragment is independently authenticated.
#[instrument(name = "reconcile.send", skip_all, fields(peer = %peer, count = messages.len()))]
pub(crate) async fn send_messages_paced<K, V, P, T>(
    messages: &[Message<K, V, P>],
    ports: &SendPorts<'_, T>,
    peer: &SocketAddr,
    send_buf: &mut Vec<u8>,
    rate: Option<usize>,
) where
    K: Serialize,
    V: Serialize,
    P: Serialize,
    T: Transport + ?Sized,
{
    debug!("sending {} messages to {peer}", messages.len());
    let Some((complete_capacity, _)) = capacities(ports.authenticator, ports.framing) else {
        observability::record_value_oversized();
        return;
    };

    let mut frame_buf = Vec::new();
    let mut pacer = Pacer::new(rate);
    send_buf.clear();

    for message in messages {
        let batch_len = send_buf.len();
        gossip::bincode::encode(message, send_buf)
            .expect("serializing a protocol Message into an in-memory buffer cannot fail");
        let message_len = send_buf.len() - batch_len;

        if send_buf.len() <= complete_capacity {
            continue;
        }

        if batch_len > 0 {
            trace!("sending {} complete payload bytes to {peer}", batch_len);
            if let Err(err) = send_logical_payload(
                ports,
                *peer,
                &send_buf[..batch_len],
                &mut frame_buf,
                &mut pacer,
            )
            .await
            {
                warn!("failed to send datagram to {peer}: {err}; continuing");
            }
        }

        let encoded_message = send_buf.split_off(batch_len);
        send_buf.clear();
        if message_len <= complete_capacity {
            send_buf.extend_from_slice(&encoded_message);
        } else if let Err(err) =
            send_logical_payload(ports, *peer, &encoded_message, &mut frame_buf, &mut pacer).await
        {
            warn!("failed to send fragmented message to {peer}: {err}; continuing");
        }
    }

    if !send_buf.is_empty() {
        trace!("sending last {} complete payload bytes to {peer}", send_buf.len());
        if let Err(err) =
            send_logical_payload(ports, *peer, send_buf, &mut frame_buf, &mut pacer).await
        {
            warn!("failed to send final datagram to {peer}: {err}; continuing");
        }
    }
}
