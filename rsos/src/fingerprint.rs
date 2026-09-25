// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! 256-bit additive range fingerprints.
//!
//! Elements are lifted with BLAKE3 over [`crate::encoding`] and combined by addition modulo
//! 2^256. The combiner is not XOR. [`LiftKey`] enables a keyed lift for adversarial settings;
//! unkeyed fingerprints provide reconciliation soundness but not chosen-input collision resistance.
//! [`FingerprintTreeMap`](crate::FingerprintTreeMap) folds each live key/value pair exactly once.

use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::encoding;

/// A 256-bit range fingerprint: four little-endian 64-bit limbs, limb 0 least significant.
/// An abelian group under addition mod 2²⁵⁶ — `+`/[`combine`](Fingerprint::combine) merges
/// disjoint ranges, `-` removes, [`ZERO`](Fingerprint::ZERO) is the identity.
/// A non-empty range can fingerprint to [`ZERO`](Fingerprint::ZERO); never decide emptiness on
/// the fingerprint, only on the element count.
/// ```
/// use rsos::{lift, Fingerprint};
/// let a = lift(&1, &"one");
/// let b = lift(&2, &"two");
/// // combine/remove are inverses -- this is what lets a range's fingerprint be maintained
/// // incrementally as elements are inserted and removed, rather than rehashed from scratch.
/// let combined = a.combine(b);
/// assert_eq!(combined.remove(b), a);
/// assert_eq!(combined.remove(a).remove(b), Fingerprint::ZERO);
/// ```
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub [u64; 4]);

// Serialize as fixed 32-byte little-endian data rather than backend-dependent integer limbs.
impl Serialize for Fingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_le_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <[u8; 32]>::deserialize(deserializer).map(|bytes| Fingerprint::from_le_bytes(&bytes))
    }
}

impl Fingerprint {
    /// The fingerprint of the empty range and the additive identity.
    pub const ZERO: Fingerprint = Fingerprint([0; 4]);

    /// Interpret 32 bytes (little-endian) as a fingerprint — the inverse of
    /// [`to_le_bytes`](Fingerprint::to_le_bytes). A third party can build a `lift`-compatible
    /// fingerprint from raw bytes (e.g. a BLAKE3 digest computed with the re-exported
    /// [`blake3`]) without reimplementing this limb decode.
    /// Unrolled rather than looped over the four limbs: a fixed count of four is simpler written
    /// out than indexed, and it keeps this `const fn` free of a manually incremented loop counter
    /// — the shape a single mutated `+=` could turn into an infinite loop, rather than a
    /// fast-failing wrong answer.
    #[must_use]
    pub const fn from_le_bytes(bytes: &[u8; 32]) -> Fingerprint {
        Fingerprint([
            u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
            u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
            u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
        ])
    }

    /// The 32-byte little-endian encoding of this fingerprint — the inverse of
    /// [`from_le_bytes`](Fingerprint::from_le_bytes). Unrolled for the same reason as
    /// `from_le_bytes` above.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 32] {
        let [a, b, c, d] = self.0;
        let a = a.to_le_bytes();
        let b = b.to_le_bytes();
        let c = c.to_le_bytes();
        let d = d.to_le_bytes();
        [
            a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], b[0], b[1], b[2], b[3], b[4], b[5],
            b[6], b[7], c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], d[0], d[1], d[2], d[3],
            d[4], d[5], d[6], d[7],
        ]
    }

    /// Combine two fingerprints (addition modulo 2²⁵⁶, with carry propagation).
    #[must_use]
    pub fn combine(self, other: Fingerprint) -> Fingerprint {
        let mut out = [0u64; 4];
        let mut carry = 0u128;
        for (o, (&a, &b)) in out.iter_mut().zip(self.0.iter().zip(other.0.iter())) {
            let sum = a as u128 + b as u128 + carry;
            *o = sum as u64;
            carry = sum >> 64;
        }
        Fingerprint(out)
    }

    /// Remove `other` from `self` (subtraction modulo 2²⁵⁶); the inverse of
    /// [`combine`](Fingerprint::combine).
    #[must_use]
    pub fn remove(self, other: Fingerprint) -> Fingerprint {
        let mut out = [0u64; 4];
        let mut borrow = 0i128;
        for (o, (&a, &b)) in out.iter_mut().zip(self.0.iter().zip(other.0.iter())) {
            let diff = a as i128 - b as i128 - borrow;
            if diff < 0 {
                *o = (diff + (1i128 << 64)) as u64;
                borrow = 1;
            } else {
                *o = diff as u64;
                borrow = 0;
            }
        }
        Fingerprint(out)
    }
}

impl Add for Fingerprint {
    type Output = Fingerprint;
    fn add(self, rhs: Fingerprint) -> Fingerprint {
        self.combine(rhs)
    }
}

impl AddAssign for Fingerprint {
    fn add_assign(&mut self, rhs: Fingerprint) {
        *self = self.combine(rhs);
    }
}

impl Sub for Fingerprint {
    type Output = Fingerprint;
    fn sub(self, rhs: Fingerprint) -> Fingerprint {
        self.remove(rhs)
    }
}

impl SubAssign for Fingerprint {
    fn sub_assign(&mut self, rhs: Fingerprint) {
        *self = self.remove(rhs);
    }
}

impl Neg for Fingerprint {
    type Output = Fingerprint;
    fn neg(self) -> Fingerprint {
        Fingerprint::ZERO.remove(self)
    }
}

impl std::fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Most-significant limb first, so the hex reads like a big-endian number.
        write!(
            f,
            "Fingerprint({:016x}{:016x}{:016x}{:016x})",
            self.0[3], self.0[2], self.0[1], self.0[0]
        )
    }
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:016x}{:016x}{:016x}{:016x}",
            self.0[3], self.0[2], self.0[1], self.0[0]
        )
    }
}

/// Key material for the keyed BLAKE3 lift: 32 bytes, opaque to `rsos`.
/// `rsos` never derives this itself — it has no notion of a cluster secret ( domain
/// purity forbids a dependency on `gossip`, which owns `ClusterKey`). `reconcile` derives it via
/// `ClusterKey::derive_lift_key` (`gossip/src/auth/key.rs`) — a BLAKE3 `derive_key` subkey, not the
/// raw cluster key, so a MAC key leak and a lift key leak stay independent — and hands the 32 bytes
/// here through [`LiftKey::new`].
/// `Clone` but not `Copy`, matching `ClusterKey`'s own reasoning: cheap to extend with a wiping
/// `Drop` later without an API break. `Debug` is redacting for the same reason a key's bytes are
/// never logged.
/// ```
/// use rsos::{lift_keyed, LiftKey};
/// let key_a = LiftKey::new([1; 32]);
/// let key_b = LiftKey::new([2; 32]);
/// // A different key lifts the same pair to a different fingerprint -- the whole point: an
/// // attacker who does not hold the key cannot predict, and therefore cannot grind, the output.
/// assert_ne!(lift_keyed(&key_a, &1, &"one"), lift_keyed(&key_b, &1, &"one"));
/// // Debug never prints the key material, even by accident.
/// assert_eq!(format!("{key_a:?}"), "LiftKey(\"<redacted>\")");
/// ```
#[derive(Clone)]
pub struct LiftKey([u8; 32]);

impl LiftKey {
    /// Wrap 32 bytes of key material as a lift key.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        LiftKey(bytes)
    }
}

impl std::fmt::Debug for LiftKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_tuple("LiftKey").field(&"<redacted>").finish()
    }
}

/// A BLAKE3 accumulator fed exclusively through the [canonical encoding](crate::encoding).
struct Blake3Hasher(blake3::Hasher);

impl Blake3Hasher {
    /// Unkeyed when `lift_key` is `None` — today's honest-model-only behavior; keyed via
    /// `blake3::Hasher::new_keyed` otherwise, exactly the primitive `gossip`'s `Blake3Mac`
    /// (`gossip/src/auth/mac.rs`) already uses for the datagram MAC.
    fn new(lift_key: Option<&LiftKey>) -> Blake3Hasher {
        Blake3Hasher(match lift_key {
            Some(key) => blake3::Hasher::new_keyed(&key.0),
            None => blake3::Hasher::new(),
        })
    }

    /// Absorb `value`'s canonical encoding.
    /// # Panics
    /// Only if a hand-written [`Serialize`] impl fails — surfaced loudly, never folded into a
    /// wrong fingerprint.
    fn absorb<T: Serialize + ?Sized>(&mut self, value: &T) {
        encoding::encode_into(&mut self.0, value).expect("canonical encoding cannot fail");
    }

    fn fingerprint(&self) -> Fingerprint {
        Fingerprint::from_le_bytes(self.0.finalize().as_bytes())
    }
}

/// Def. 3.4's lifting function `lift: U → M`, optionally keyed: BLAKE3 (keyed when `lift_key` is
/// `Some`, per [`Blake3Hasher::new`]) over the [canonically encoded](crate::encoding) key followed
/// by the canonically encoded value.
/// The shared implementation behind [`lift`]/[`lift_keyed`] (`lift_key: None`/`Some` respectively)
/// and `FingerprintTreeMap`'s internals, which reach it directly to avoid re-deriving `Option`
/// dispatch at every one of the tree's own lift sites.
pub(crate) fn lift_with<K: Serialize + ?Sized, V: Serialize + ?Sized>(
    lift_key: Option<&LiftKey>,
    key: &K,
    value: &V,
) -> Fingerprint {
    let mut hasher = Blake3Hasher::new(lift_key);
    hasher.absorb(key);
    hasher.absorb(value);
    hasher.fingerprint()
}

/// `lift_with` with no key — the honest-model-only lift this module's doc explains at length.
/// Part of the wire protocol — see this module's golden vectors. The [`Serialize`] bound admits
/// keys and values std implements no [`Hash`](std::hash::Hash) for
/// ([`HashMap`](std::collections::HashMap), [`HashSet`](std::collections::HashSet)).
/// ```
/// use rsos::lift;
/// // Injective within a type: changing either half of the pair moves the fingerprint.
/// assert_ne!(lift(&1, &"a"), lift(&2, &"a"));
/// assert_ne!(lift(&1, &"a"), lift(&1, &"b"));
/// // Deterministic: the same pair always lifts to the same fingerprint.
/// assert_eq!(lift(&1, &"a"), lift(&1, &"a"));
/// ```
pub fn lift<K: Serialize + ?Sized, V: Serialize + ?Sized>(key: &K, value: &V) -> Fingerprint {
    lift_with(None, key, value)
}

/// [`lift`], keyed under `lift_key`. The same `(key, value)` pair lifts to an unrelated
/// fingerprint under every distinct key, so a chosen-input adversary without `lift_key` cannot
/// predict, and therefore cannot grind, a collision — see this module's doc.
/// ```
/// use rsos::{lift_keyed, LiftKey};
/// let key = LiftKey::new([7; 32]);
/// // Still injective and deterministic within one key, exactly like the unkeyed `lift`.
/// assert_ne!(lift_keyed(&key, &1, &"a"), lift_keyed(&key, &2, &"a"));
/// assert_eq!(lift_keyed(&key, &1, &"a"), lift_keyed(&key, &1, &"a"));
/// ```
pub fn lift_keyed<K: Serialize + ?Sized, V: Serialize + ?Sized>(
    lift_key: &LiftKey,
    key: &K,
    value: &V,
) -> Fingerprint {
    lift_with(Some(lift_key), key, value)
}

/// The canonical 256-bit digest of a single value — [`lift`] with no key half, same encoding.
pub fn digest<T: Serialize + ?Sized>(value: &T) -> Fingerprint {
    lift_with(None, &(), value)
}

/// [`digest`], keyed under `lift_key` — [`lift_keyed`] with no key half, same encoding.
pub fn digest_keyed<T: Serialize + ?Sized>(lift_key: &LiftKey, value: &T) -> Fingerprint {
    lift_with(Some(lift_key), &(), value)
}

#[cfg(test)]
mod tests;
