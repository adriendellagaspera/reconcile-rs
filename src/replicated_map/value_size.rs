// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;

use crate::bounds::Value;

/// One reason a [`try_insert`](super::ReplicatedMap::try_insert)/
/// [`try_update`](super::ReplicatedMap::try_update) write can be
/// [`WriteRejected`](super::WriteRejected): the value's encoded size exceeds
/// [`Config::max_value_size`](super::Config::max_value_size) (#82).
///
/// The infallible [`insert`](super::ReplicatedMap::insert)/[`update`](super::ReplicatedMap::update)
/// never consult `max_value_size` and so never reject on it — see their docs.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub struct ValueTooLarge {
    /// The value's encoded size, in bytes.
    pub encoded_size: usize,
    /// The [`Config::max_value_size`](super::Config::max_value_size) it exceeded.
    pub max_value_size: usize,
}

impl fmt::Display for ValueTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "value encodes to {} bytes, exceeding Config::max_value_size ({} bytes)",
            self.encoded_size, self.max_value_size
        )
    }
}

impl std::error::Error for ValueTooLarge {}

/// Encode `value` the same way the send path does ([`gossip::bincode::encode`], the codec
/// `replica::pacing` frames every message with) and, when `max_value_size` is set, reject it
/// before the caller's write reaches any local state — #82's write-time counterpart to the
/// send-time drop `replica::pacing` already logs and counts (`VALUES_OVERSIZED_TOTAL`) once a key
/// like this can never converge on any peer.
pub(super) fn check_value_size<V: Value>(
    value: &V,
    max_value_size: Option<usize>,
) -> Result<(), ValueTooLarge> {
    let Some(max_value_size) = max_value_size else {
        return Ok(());
    };
    let mut buf = Vec::new();
    gossip::bincode::encode(value, &mut buf)
        .expect("serializing a value into an in-memory buffer cannot fail");
    let encoded_size = buf.len();
    if encoded_size > max_value_size {
        Err(ValueTooLarge {
            encoded_size,
            max_value_size,
        })
    } else {
        Ok(())
    }
}
