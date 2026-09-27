use super::*;

/// Project a one-way rateless stream with a reverse stop signal.
///
/// decoded_app_bytes is how many coded-symbol bytes must arrive before the receiver can decode.
/// In the full-rate steady-state envelope the sender transmits for one full RTT after those bytes
/// cross the receiver: one half-RTT of bytes were already in flight, then another half-RTT is sent
/// before the stop signal arrives. That is one forward bandwidth-delay product of overshoot.
///
/// This intentionally does not model congestion-window startup. It is the reusable/full-rate case;
/// cold-start transport handshakes are still priced by transport.handshake_rtts.
pub(super) fn estimate_rateless_stop(
    decoded_app_bytes: usize,
    stop_app_bytes: usize,
    link: LinkProfile,
    transport: TransportProfile,
) -> RatelessEstimate {
    let discovery = estimate(
        Exchange {
            forward_app_bytes: decoded_app_bytes,
            reverse_app_bytes: 0,
            interaction_rtts: 0.5,
            logical_messages: 1,
        },
        link,
        transport,
    );

    let bdp = link.forward_mbps * 1_000_000.0 / 8.0 * (link.rtt_ms / 1_000.0);
    let overshoot = bdp.ceil() as usize;

    let quiescence = estimate(
        Exchange {
            forward_app_bytes: decoded_app_bytes + overshoot,
            reverse_app_bytes: stop_app_bytes,
            interaction_rtts: 1.0,
            logical_messages: 2,
        },
        link,
        transport,
    );

    RatelessEstimate {
        discovery,
        quiescence,
        stop_overshoot_app_bytes: overshoot,
    }
}
