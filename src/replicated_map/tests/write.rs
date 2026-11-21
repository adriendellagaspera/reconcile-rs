// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::{virtual_config, virtual_map};

/// `just_insert_bulk` must actually insert every pair, not silently no-op.
#[tokio::test]
async fn just_insert_bulk_actually_inserts_every_pair() {
    let store = virtual_map::<i32, i32>(virtual_config());
    store.just_insert_bulk(&[(1, 10), (2, 20), (3, 30)]);
    assert_eq!(store.get(&1).as_deref(), Some(&10));
    assert_eq!(store.get(&2).as_deref(), Some(&20));
    assert_eq!(store.get(&3).as_deref(), Some(&30));
}

/// `just_remove_bulk` must actually remove every key, not silently no-op.
#[tokio::test]
async fn just_remove_bulk_actually_removes_every_key() {
    let store = virtual_map::<i32, i32>(virtual_config());
    store.just_insert_bulk(&[(1, 10), (2, 20)]);
    store.just_remove_bulk(&[1, 2]);
    assert_eq!(store.get(&1).as_deref(), None);
    assert_eq!(store.get(&2).as_deref(), None);
}
