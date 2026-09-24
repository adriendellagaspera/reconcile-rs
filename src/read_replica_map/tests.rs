// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::replicated_map::Config;

mod discovery;
mod keyed_fingerprint;
mod lifecycle;
mod membership;
mod read;
mod value_ref;
mod write;

fn ephemeral_config() -> Config {
    // A fresh port per call — Config::port must be nonzero — on the loopback default
    // network.
    Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_insecure_no_key()
}

fn virtual_config() -> Config {
    Config::default()
        .with_port(5000)
        .with_insecure_no_key()
}

fn isolated_read_replica<K: crate::bounds::Key, V: crate::bounds::Value>(
    config: Config,
) -> super::ReadReplicaMap<K, V> {
    use std::net::SocketAddr;
    use std::sync::Arc;

    let network = crate::transport::InMemoryNetwork::new();
    let endpoint = SocketAddr::new(config.listen_addr, config.port);
    super::ReadReplicaMap::new_with_transport(config, Arc::new(network.bind(endpoint)))
        .expect("valid test configuration")
}

async fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}
