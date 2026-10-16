// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Datagram authentication and strict wire framing.
//!
//! Wire layouts:
//! - Unauthenticated: `version(1) || protocol_messages`.
//! - MAC: `tag(32) || seq(8 LE) || stamp(8 LE) || version(1) || protocol_messages`.
//! - Encryption: `nonce(24) || AEAD(seq(8 LE) || stamp(8 LE) || version(1) || protocol_messages) || tag(16)`.
//!
//! Version validation follows authentication and precedes replay-state allocation.
//! Message handling requires `Payload<Verified>`; authentication alone cannot admit traffic.

use std::borrow::Cow;
use std::marker::PhantomData;

use crate::replay::{Seq, Stamp};

#[cfg(feature = "encryption")]
mod encryption;
mod key;
mod mac;
mod open;
mod seal;

/// Length in bytes of the authentication tag prepended to every datagram.
pub const TAG_LEN: usize = 32;

/// Length in bytes of a cluster key.
pub const KEY_LEN: usize = 32;

/// Length in bytes of the wire-version byte prepended to every datagram's protected region,
/// regardless of authentication mode.
pub const VERSION_LEN: usize = 1;

/// The wire protocol version this build produces and accepts.
/// Bumping it is the sanctioned way to make a non-additive change to the `Message` wire format:
/// a peer running a different version is rejected with a distinguishable, counted reason
/// (`reconcile_datagrams_dropped_total{reason="version"}`) rather than silently misread or
/// indistinguishably dropped as malformed. There is no accepted-version *window* — a
/// mismatch of any kind is rejected; widening that is a policy change to make deliberately, not a
/// side effect of the next bump.
pub const WIRE_VERSION: u8 = 4;

/// Length in bytes of the XChaCha20-Poly1305 nonce prepended to each encrypted datagram.
/// 192 bits: safe to draw at random per datagram, so no per-peer counter is needed.
#[cfg(feature = "encryption")]
pub const AEAD_NONCE_LEN: usize = 24;

/// Length in bytes of the XChaCha20-Poly1305 (Poly1305) authentication tag.
#[cfg(feature = "encryption")]
pub const AEAD_TAG_LEN: usize = 16;

#[cfg(not(any(feature = "mac-blake3", feature = "mac-hmac")))]
compile_error!(
    "gossip: no MAC backend selected. Enable feature `mac-blake3` (default) or `mac-hmac` — \
     either on this crate directly, or via the identically-named unification feature on `reconcile`."
);

/// A shared cluster secret. Constructing one is the only way to enable authentication.
/// `Clone` but not `Copy`: the `zeroize` feature gives it a wiping `Drop`, which `Copy` forbids.
/// The public boundary (`Config::cluster_key`, `Authenticator::new`) takes and returns
/// `ClusterKey`, never a bare `[u8; 32]`; invalid key material cannot be constructed.
/// `Debug` is redacting: it never prints the key material, so an accidental `{:?}` in a log
/// statement cannot leak it.
#[cfg_attr(feature = "zeroize", derive(zeroize::Zeroize, zeroize::ZeroizeOnDrop))]
#[derive(Clone)]
pub struct ClusterKey([u8; KEY_LEN]);

/// Why constructing a [`ClusterKey`] from untrusted input failed.
#[derive(Debug, Eq, PartialEq)]
pub enum ClusterKeyError {
    /// [`ClusterKey::from_hex`] got a string that was not exactly `2 * KEY_LEN` (64) characters.
    WrongHexLength(usize),
    /// [`ClusterKey::from_hex`] got a character outside `[0-9a-fA-F]`.
    InvalidHexDigit,
    /// [`TryFrom<&[u8]>`](ClusterKey#impl-TryFrom<%26[u8]>-for-ClusterKey) got a slice that was
    /// not exactly `KEY_LEN` (32) bytes.
    WrongByteLength(usize),
}

/// Returned by [`Authenticator::new`]/[`with_rotation`](Authenticator::with_rotation) when
/// `encrypt` is requested but the crate was built without the `encryption` feature.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub struct EncryptionFeatureDisabled;

/// [`Payload`] state: cleared the MAC/AEAD gate, but not yet checked against the per-peer replay
/// filter. The only state [`Authenticator::open`] can produce.
pub struct Authenticated;

/// [`Payload`] state: cleared the replay filter too (or exempt because the connection runs
/// unauthenticated). The only state [`Payload::as_bytes`] accepts, and the only state message
/// handling (`handle_messages`) accepts — obtainable solely through
/// [`Payload::verify_replay`].
pub struct Verified;

/// A datagram payload that has cleared the authentication gate, obtainable only from
/// [`Authenticator::open`].
/// `State` is [`Authenticated`] or [`Verified`], so the order of the two checks is a compile-time
/// property. `seq`/`stamp` are [`Seq::NONE`]/[`Stamp::NONE`] in unauthenticated mode. [`Cow`]
/// because the MAC path borrows the receive buffer and the encrypted path owns a plaintext.
#[derive(Debug)]
pub struct Payload<'a, State = Authenticated> {
    bytes: Cow<'a, [u8]>,
    /// Sender sequence number extracted from the replay header, or [`Seq::NONE`] in
    /// unauthenticated mode.
    pub seq: Seq,
    /// Sender wall-clock stamp from the replay header, or [`Stamp::NONE`] in unauthenticated
    /// mode.
    pub stamp: Stamp,
    _state: PhantomData<State>,
}

/// A [`ClusterKey`] this node seals outgoing datagrams with, plus zero or more additional keys it
/// still accepts on the verify path — the shape a rotation needs: roll out `also_accept:
/// [old_key]` cluster-wide, then once every peer has it, roll `primary` to the new key with the
/// old one demoted to `also_accept`, then finally drop it once every peer is on the new primary.
#[derive(Clone, Debug)]
pub struct Keys {
    /// The key `seal` always uses.
    pub primary: ClusterKey,
    /// Additional keys `open` accepts, tried in order after `primary` fails. Empty outside a
    /// rotation.
    pub also_accept: Vec<ClusterKey>,
}

/// Authentication policy and datagram framing for one node: the sole producer of [`Payload`].
#[derive(Clone, Debug)]
pub enum Authenticator {
    /// No cluster key configured: the protocol runs unauthenticated.
    Disabled,
    /// A cluster key is configured: datagrams are MAC-sealed and verified (plaintext payload).
    Enabled(Keys),
    /// A cluster key is configured and the `encryption` feature is active: datagrams are
    /// authenticated *and* encrypted with XChaCha20-Poly1305 over the cluster key.
    #[cfg(feature = "encryption")]
    Encrypted(Keys),
}

#[cfg(test)]
mod tests;
