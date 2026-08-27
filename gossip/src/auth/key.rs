// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`ClusterKey`]/[`ClusterKeyError`]/[`Keys`] construction, plus [`Authenticator`]'s own
//! construction and size accounting — everything about *which* keys are in play, as opposed to
//! `seal`/`open`'s per-datagram use of them.

use std::fmt;

use super::{
    Authenticator, ClusterKey, ClusterKeyError, EncryptionFeatureDisabled, Keys, KEY_LEN, TAG_LEN,
    VERSION_LEN,
};
#[cfg(feature = "encryption")]
use super::{AEAD_NONCE_LEN, AEAD_TAG_LEN};
use crate::replay::REPLAY_HEADER_LEN;

impl ClusterKey {
    /// Wrap a raw 32-byte secret as a cluster key.
    pub fn new(bytes: [u8; KEY_LEN]) -> Self {
        ClusterKey(bytes)
    }

    /// Parse a cluster key from `2 * KEY_LEN` (64) hex characters, case-insensitive.
    ///
    /// The one parse this type exists to own — see AGENTS.md §4 — rather than every caller
    /// hand-rolling `u8::from_str_radix` over byte pairs (as, until #286, `examples/k8s/main.rs`
    /// did for `RECONCILE_CLUSTER_KEY`).
    pub fn from_hex(hex: &str) -> Result<Self, ClusterKeyError> {
        if hex.len() != KEY_LEN * 2 {
            return Err(ClusterKeyError::WrongHexLength(hex.len()));
        }
        let mut bytes = [0u8; KEY_LEN];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|_| ClusterKeyError::InvalidHexDigit)?;
        }
        Ok(ClusterKey(bytes))
    }

    pub(super) fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// Derive a 32-byte subkey for `rsos`'s keyed range-fingerprint lift, independent of the
    /// datagram MAC this key also seals — a BLAKE3 `derive_key` context-separated subkey, not
    /// these raw bytes, so a leak of one purpose's key does not hand over the other's.
    ///
    /// Returns raw bytes rather than an `rsos::LiftKey`: `gossip` cannot depend on `rsos` (AGENTS.md
    /// §9 — no edge between the two adapter/leaf crates in `ARCHITECTURE.md` §2's graph), so
    /// `reconcile`, which depends on both, is the one that wraps the result with
    /// `rsos::LiftKey::new`.
    ///
    /// ```
    /// use reconcile_gossip::auth::ClusterKey;
    ///
    /// let key = ClusterKey::new([7; 32]);
    ///
    /// // Deterministic: the same cluster key always derives the same lift key, which is what lets
    /// // every node in the cluster compute matching fingerprints independently.
    /// assert_eq!(key.derive_lift_key(), key.derive_lift_key());
    ///
    /// // Independent of the raw cluster key bytes -- not just those bytes echoed back.
    /// assert_ne!(key.derive_lift_key(), [7; 32]);
    /// ```
    #[must_use]
    pub fn derive_lift_key(&self) -> [u8; KEY_LEN] {
        blake3::derive_key(
            "reconcile-rs 2026-08-25 rsos::fingerprint lift key",
            &self.0,
        )
    }
}

impl fmt::Debug for ClusterKey {
    /// Redacted: never prints the key material, whatever the format flags.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ClusterKey").field(&"<redacted>").finish()
    }
}

impl TryFrom<&[u8]> for ClusterKey {
    type Error = ClusterKeyError;

    /// `bytes` must be exactly `KEY_LEN` (32) bytes long.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        <[u8; KEY_LEN]>::try_from(bytes)
            .map(ClusterKey)
            .map_err(|_| ClusterKeyError::WrongByteLength(bytes.len()))
    }
}

impl fmt::Display for ClusterKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterKeyError::WrongHexLength(got) => write!(
                f,
                "cluster key must be {} hex characters, got {got}",
                KEY_LEN * 2
            ),
            ClusterKeyError::InvalidHexDigit => {
                write!(
                    f,
                    "cluster key hex string contains a non-hex-digit character"
                )
            }
            ClusterKeyError::WrongByteLength(got) => {
                write!(f, "cluster key must be {KEY_LEN} bytes, got {got}")
            }
        }
    }
}

impl std::error::Error for ClusterKeyError {}

impl fmt::Display for EncryptionFeatureDisabled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reconcile: encryption requested but the crate was built without the `encryption` feature"
        )
    }
}

impl std::error::Error for EncryptionFeatureDisabled {}

impl Keys {
    /// A single key, accepting nothing else — the common, non-rotating case.
    pub fn single(key: ClusterKey) -> Keys {
        Keys {
            primary: key,
            also_accept: Vec::new(),
        }
    }

    /// `primary`, then each `also_accept` key in order.
    pub(super) fn iter(&self) -> impl Iterator<Item = &ClusterKey> {
        std::iter::once(&self.primary).chain(self.also_accept.iter())
    }
}

impl Authenticator {
    /// Build an authenticator from an optional cluster key and whether to encrypt. No rotation:
    /// see [`with_rotation`](Self::with_rotation) to also accept prior keys on the verify path.
    ///
    /// # Panics
    ///
    /// If `encrypt` is `true` and the crate was built without the `encryption` feature — a loud
    /// failure rather than a silent downgrade. See [`try_new`](Self::try_new) for a non-panicking
    /// alternative.
    pub fn new(key: Option<ClusterKey>, encrypt: bool) -> Self {
        Self::with_rotation(key.map(Keys::single), encrypt)
    }

    /// As [`new`](Self::new), but returns an [`EncryptionFeatureDisabled`] instead of panicking
    /// when `encrypt` is `true` and the crate was built without the `encryption` feature.
    ///
    /// # Errors
    ///
    /// If `encrypt` is `true` and the crate was built without the `encryption` feature.
    pub fn try_new(
        key: Option<ClusterKey>,
        encrypt: bool,
    ) -> Result<Self, EncryptionFeatureDisabled> {
        Self::try_with_rotation(key.map(Keys::single), encrypt)
    }

    /// Build an authenticator from an optional [`Keys`] (a primary key to seal with, plus
    /// prior keys still accepted on the verify path — #285/#137) and whether to encrypt.
    ///
    /// # Panics
    ///
    /// If `encrypt` is `true` and the crate was built without the `encryption` feature — a loud
    /// failure rather than a silent downgrade. See [`try_with_rotation`](Self::try_with_rotation)
    /// for a non-panicking alternative.
    pub fn with_rotation(keys: Option<Keys>, encrypt: bool) -> Self {
        match Self::try_with_rotation(keys, encrypt) {
            Ok(auth) => auth,
            Err(err) => panic!("{err}"),
        }
    }

    /// As [`with_rotation`](Self::with_rotation), but returns an [`EncryptionFeatureDisabled`]
    /// instead of panicking when `encrypt` is `true` and the crate was built without the
    /// `encryption` feature.
    ///
    /// # Errors
    ///
    /// If `encrypt` is `true` and the crate was built without the `encryption` feature.
    pub fn try_with_rotation(
        keys: Option<Keys>,
        encrypt: bool,
    ) -> Result<Self, EncryptionFeatureDisabled> {
        Ok(match (keys, encrypt) {
            (None, _) => Authenticator::Disabled,
            (Some(keys), false) => Authenticator::Enabled(keys),
            #[cfg(feature = "encryption")]
            (Some(keys), true) => Authenticator::Encrypted(keys),
            #[cfg(not(feature = "encryption"))]
            (Some(_), true) => return Err(EncryptionFeatureDisabled),
        })
    }

    /// Extra bytes a sealed datagram adds over the raw messages, for MTU accounting: crypto
    /// overhead plus the replay header, plus the wire-version byte present in every mode.
    pub fn overhead(&self) -> usize {
        match self {
            Authenticator::Disabled => VERSION_LEN,
            Authenticator::Enabled(_) => TAG_LEN + VERSION_LEN + REPLAY_HEADER_LEN,
            #[cfg(feature = "encryption")]
            Authenticator::Encrypted(_) => {
                AEAD_NONCE_LEN + VERSION_LEN + REPLAY_HEADER_LEN + AEAD_TAG_LEN
            }
        }
    }
}
