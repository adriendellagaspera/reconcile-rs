// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.


use super::{virtual_config, virtual_map};

#[tokio::test]
async fn value_ref_pins_the_observed_value_across_later_writes() {
    let store = virtual_map::<i32, String>(virtual_config());
    store.insert(1, "old".to_string());

    let old = store.get(&1).expect("live value");
    store.insert(1, "new".to_string());

    assert_eq!(old.as_str(), "old");
    assert_eq!(store.get_cloned(&1), Some("new".to_string()));

    store.remove(&1);
    assert_eq!(old.as_str(), "old");
    assert!(store.get(&1).is_none());
}
