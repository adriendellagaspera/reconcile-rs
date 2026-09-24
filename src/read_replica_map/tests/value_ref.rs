// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::entry::State;

use super::{isolated_read_replica, virtual_config};

#[tokio::test]
async fn value_ref_pins_the_observed_value_across_later_integrations() {
    let replica = isolated_read_replica::<i32, String>(virtual_config());
    replica.integrate(vec![(1, State::Present("old".to_string()))]);

    let old = replica.get(&1).expect("live value");
    replica.integrate(vec![(1, State::Present("new".to_string()))]);

    assert_eq!(old.as_str(), "old");
    assert_eq!(replica.get_cloned(&1), Some("new".to_string()));

    replica.integrate(vec![(1, State::Tombstone)]);
    assert_eq!(old.as_str(), "old");
    assert!(replica.get(&1).is_none());
}
