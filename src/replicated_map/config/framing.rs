// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::Duration;

/// Default total UDP datagram payload budget, including authentication/version framing.
///
/// 1200 bytes remains below the IPv6 minimum-MTU UDP payload ceiling (1232 bytes).
pub const DEFAULT_DATAGRAM_PAYLOAD_BUDGET: usize = 1_200;

/// UDP application-framing and incomplete-message reassembly policy.
///
/// Logical protocol messages larger than one complete frame are content-addressed and split across
/// independently authenticated datagrams. Reassembly is bounded per transfer, per peer, globally,
/// and by inactivity time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FramingConfig {
    /// Maximum bytes handed to the UDP transport for one datagram, including auth overhead.
    pub datagram_payload_budget: usize,
    /// Maximum encoded bytes in one logical protocol message.
    pub max_logical_message_size: usize,
    /// Maximum fragments retained for one logical message.
    pub max_fragments_per_message: usize,
    /// Maximum incomplete logical messages retained for one peer.
    pub max_incomplete_transfers_per_peer: usize,
    /// Maximum fragment bytes retained for one peer.
    pub max_reassembly_bytes_per_peer: usize,
    /// Maximum fragment bytes retained across all peers.
    pub max_total_reassembly_bytes: usize,
    /// Inactivity timeout for incomplete transfers.
    pub reassembly_ttl: Duration,
}

impl Default for FramingConfig {
    fn default() -> Self {
        FramingConfig {
            datagram_payload_budget: DEFAULT_DATAGRAM_PAYLOAD_BUDGET,
            max_logical_message_size: 8 * 1024 * 1024,
            max_fragments_per_message: 8_192,
            max_incomplete_transfers_per_peer: 8,
            max_reassembly_bytes_per_peer: 16 * 1024 * 1024,
            max_total_reassembly_bytes: 64 * 1024 * 1024,
            reassembly_ttl: Duration::from_secs(30),
        }
    }
}

impl FramingConfig {
    /// Set the total UDP payload budget, including authentication/version framing.
    #[must_use]
    pub fn with_datagram_payload_budget(mut self, bytes: usize) -> Self {
        self.datagram_payload_budget = bytes;
        self
    }

    /// Set the maximum encoded logical-message size.
    #[must_use]
    pub fn with_max_logical_message_size(mut self, bytes: usize) -> Self {
        self.max_logical_message_size = bytes;
        self
    }

    /// Set the maximum fragments retained for one logical message.
    #[must_use]
    pub fn with_max_fragments_per_message(mut self, max: usize) -> Self {
        self.max_fragments_per_message = max;
        self
    }

    /// Set the maximum incomplete transfers retained for one peer.
    #[must_use]
    pub fn with_max_incomplete_transfers_per_peer(mut self, max: usize) -> Self {
        self.max_incomplete_transfers_per_peer = max;
        self
    }

    /// Set the maximum fragment bytes retained for one peer.
    #[must_use]
    pub fn with_max_reassembly_bytes_per_peer(mut self, bytes: usize) -> Self {
        self.max_reassembly_bytes_per_peer = bytes;
        self
    }

    /// Set the maximum fragment bytes retained globally.
    #[must_use]
    pub fn with_max_total_reassembly_bytes(mut self, bytes: usize) -> Self {
        self.max_total_reassembly_bytes = bytes;
        self
    }

    /// Set the incomplete-transfer inactivity timeout.
    #[must_use]
    pub fn with_reassembly_ttl(mut self, ttl: Duration) -> Self {
        self.reassembly_ttl = ttl;
        self
    }

    pub(crate) fn validate(self, auth_overhead: usize) -> Result<(), super::ConfigError> {
        const MAX_UDP_PAYLOAD: usize = 65_507;
        let minimum = auth_overhead
            .saturating_add(gossip::framing::FRAGMENT_HEADER_LEN)
            .saturating_add(1);
        if self.datagram_payload_budget < minimum {
            return Err(super::ConfigError::DatagramPayloadBudgetTooSmall {
                configured: self.datagram_payload_budget,
                minimum,
            });
        }
        if self.datagram_payload_budget > MAX_UDP_PAYLOAD {
            return Err(super::ConfigError::DatagramPayloadBudgetTooLarge {
                configured: self.datagram_payload_budget,
                maximum: MAX_UDP_PAYLOAD,
            });
        }
        if self.max_logical_message_size > u32::MAX as usize {
            return Err(super::ConfigError::LogicalMessageSizeTooLarge {
                configured: self.max_logical_message_size,
                maximum: u32::MAX as usize,
            });
        }
        Ok(())
    }
    pub(crate) fn reassembly_limits(self) -> gossip::framing::ReassemblyLimits {
        gossip::framing::ReassemblyLimits {
            max_logical_message_size: self.max_logical_message_size,
            max_fragments_per_message: self.max_fragments_per_message,
            max_incomplete_transfers_per_peer: self.max_incomplete_transfers_per_peer,
            max_reassembly_bytes_per_peer: self.max_reassembly_bytes_per_peer,
            max_total_reassembly_bytes: self.max_total_reassembly_bytes,
            reassembly_ttl: self.reassembly_ttl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_is_ipv6_minimum_mtu_safe() {
        assert!(FramingConfig::default().datagram_payload_budget <= 1_232);
    }

    #[test]
    fn builders_change_only_the_selected_limit() {
        let default = FramingConfig::default();
        let configured = default
            .with_datagram_payload_budget(900)
            .with_max_logical_message_size(1024)
            .with_max_fragments_per_message(4)
            .with_max_incomplete_transfers_per_peer(2)
            .with_max_reassembly_bytes_per_peer(2048)
            .with_max_total_reassembly_bytes(4096)
            .with_reassembly_ttl(Duration::from_secs(5));
        assert_eq!(configured.datagram_payload_budget, 900);
        assert_eq!(configured.max_logical_message_size, 1024);
        assert_eq!(configured.max_fragments_per_message, 4);
        assert_eq!(configured.max_incomplete_transfers_per_peer, 2);
        assert_eq!(configured.max_reassembly_bytes_per_peer, 2048);
        assert_eq!(configured.max_total_reassembly_bytes, 4096);
        assert_eq!(configured.reassembly_ttl, Duration::from_secs(5));
    }

    #[test]
    fn validation_boundaries_are_inclusive_at_exact_limits() {
        use super::super::ConfigError;

        let auth_overhead = 17;
        let minimum = auth_overhead + gossip::framing::FRAGMENT_HEADER_LEN + 1;
        assert_eq!(
            FramingConfig::default()
                .with_datagram_payload_budget(minimum)
                .validate(auth_overhead),
            Ok(())
        );
        assert_eq!(
            FramingConfig::default()
                .with_datagram_payload_budget(minimum - 1)
                .validate(auth_overhead),
            Err(ConfigError::DatagramPayloadBudgetTooSmall {
                configured: minimum - 1,
                minimum,
            })
        );

        const MAX_UDP_PAYLOAD: usize = 65_507;
        assert_eq!(
            FramingConfig::default()
                .with_datagram_payload_budget(MAX_UDP_PAYLOAD)
                .validate(auth_overhead),
            Ok(())
        );
        assert_eq!(
            FramingConfig::default()
                .with_datagram_payload_budget(MAX_UDP_PAYLOAD + 1)
                .validate(auth_overhead),
            Err(ConfigError::DatagramPayloadBudgetTooLarge {
                configured: MAX_UDP_PAYLOAD + 1,
                maximum: MAX_UDP_PAYLOAD,
            })
        );

        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(
                FramingConfig::default()
                    .with_max_logical_message_size(u32::MAX as usize)
                    .validate(auth_overhead),
                Ok(())
            );
            assert_eq!(
                FramingConfig::default()
                    .with_max_logical_message_size(u32::MAX as usize + 1)
                    .validate(auth_overhead),
                Err(ConfigError::LogicalMessageSizeTooLarge {
                    configured: u32::MAX as usize + 1,
                    maximum: u32::MAX as usize,
                })
            );
        }
    }
}
