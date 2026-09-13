// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::super::FingerprintTreeMap;

#[test]
fn owned_ref_survives_overwrite_and_removal() {
    let mut map = FingerprintTreeMap::new();
    for key in 0..100 {
        map.insert(key, key * 10);
    }

    let old = map.get_owned(&50).expect("present key");
    assert_eq!(*old, 500);

    map.insert(50, 999);
    assert_eq!(*old, 500, "handle must pin the version it observed");
    assert_eq!(map.get(&50), Some(&999));

    map.remove(&50);
    assert_eq!(*old, 500, "later removal must not invalidate the handle");
    assert_eq!(map.get(&50), None);
}

#[test]
fn owned_ref_accepts_borrowed_keys() {
    let mut map: FingerprintTreeMap<String, u32> = FingerprintTreeMap::new();
    map.insert("alpha".into(), 1);
    map.insert("beta".into(), 2);

    assert_eq!(map.get_owned("beta").as_deref(), Some(&2));
    assert!(map.get_owned("missing").is_none());
}
