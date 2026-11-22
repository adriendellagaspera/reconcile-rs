// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Provides utility methods to generate IP addresses from a CIDR.

use ipnet::{IpBitAnd, IpBitOr, IpNet, Ipv4Net, Ipv6Net};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use rand::Rng;

/// Select a random IP address from a CIDR network.
fn gen_ip<R: Rng>(rng: &mut R, network: IpNet) -> IpAddr {
    match network {
        IpNet::V4(network) => IpAddr::V4(gen_ipv4(rng, network)),
        IpNet::V6(network) => IpAddr::V6(gen_ipv6(rng, network)),
    }
}

fn gen_ipv4<R: Rng>(rng: &mut R, network: Ipv4Net) -> Ipv4Addr {
    let random: Ipv4Addr = rng.gen::<u32>().into();
    network.network().bitor(random.bitand(network.hostmask()))
}

fn gen_ipv6<R: Rng>(rng: &mut R, network: Ipv6Net) -> Ipv6Addr {
    let random: Ipv6Addr = rng.gen::<u128>().into();
    network.network().bitor(random.bitand(network.hostmask()))
}

/// One random probe address per network CIDR, so discovery spans every configured network.
pub(crate) fn probe_targets<R: Rng>(rng: &mut R, nets: &[IpNet]) -> Vec<IpAddr> {
    nets.iter().map(|&net| gen_ip(rng, net)).collect()
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::gen_ip;

    const N_ADDR: usize = 1000;

    #[test]
    fn random_addresses_stay_inside_ipv4_and_ipv6_networks() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let ipv4 = "127.0.0.0/8".parse().unwrap();
        let ipv6 = "2001:db8::/32".parse().unwrap();

        for _ in 0..N_ADDR {
            let v4 = gen_ip(&mut rng, ipv4);
            let v6 = gen_ip(&mut rng, ipv6);
            assert!(ipv4.contains(&v4), "{ipv4} should contain {v4}");
            assert!(ipv6.contains(&v6), "{ipv6} should contain {v6}");
        }
    }

    #[test]
    fn probe_targets_one_per_net() {
        use super::probe_targets;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let nets = [
            "127.0.0.0/30".parse().unwrap(),
            "127.0.1.0/30".parse().unwrap(),
            "10.0.0.0/8".parse().unwrap(),
        ];
        let targets = probe_targets(&mut rng, &nets);
        assert_eq!(targets.len(), nets.len());
        for (target, net) in targets.iter().zip(nets.iter()) {
            assert!(net.contains(target), "{net} should contain {target}");
        }
    }
}
