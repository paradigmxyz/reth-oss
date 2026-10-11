//! Discovery dialing follows the node's explicit enabled IP families.

use super::*;
use libp2p::multiaddr::Protocol;

/// A single-family listener never initiates an unsupported-family QUIC or TCP connection.
#[derive(Clone, Copy, Debug)]
pub(super) struct DialFamilies {
    ipv4: bool,
    ipv6: bool,
}

impl DialFamilies {
    pub(super) const fn from_listen(listen: &ListenConfig) -> Self {
        Self {
            ipv4: matches!(listen, ListenConfig::Ipv4 { .. } | ListenConfig::DualStack { .. }),
            ipv6: matches!(listen, ListenConfig::Ipv6 { .. } | ListenConfig::DualStack { .. }),
        }
    }

    pub(super) fn retain(self, addresses: &[Multiaddr]) -> Vec<Multiaddr> {
        addresses
            .iter()
            .filter(|address| match address.iter().next() {
                Some(Protocol::Ip4(_)) => self.ipv4,
                Some(Protocol::Ip6(_)) => self.ipv6,
                _ => false,
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{discovery::DiscoveryRecord, DiscoveryForkFields};
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn signed_candidate(ipv4: bool) -> DiscoveryRecord {
        let config = ChainConfig::hoodi();
        let slot = 50700 * 32;
        let fields =
            DiscoveryForkFields::from_config(&config, slot / config.slots_per_epoch).unwrap();
        let eth2 = fields.eth2.to_ssz();
        let key = CombinedKey::generate_secp256k1();
        let mut builder = Enr::builder();
        builder
            .ip6(Ipv6Addr::LOCALHOST)
            .udp6(9000)
            .tcp6(9000)
            .add_value("quic6", &9001u16)
            .add_value("eth2", &eth2.as_slice());
        if ipv4 {
            builder.ip4(Ipv4Addr::LOCALHOST).udp4(9000).tcp4(9000).add_value("quic", &9001u16);
        }
        if let Some(digest) = fields.next_fork_digest {
            let custody = crate::encode_custody_group_count(0);
            builder.add_value("nfd", &digest.as_slice()).add_value("cgc", &custody.as_slice());
        }
        DiscoveryRecord::verify(
            builder.build(&key).unwrap(),
            &config,
            slot,
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap()
    }

    #[test]
    fn signed_dual_stack_candidate_retains_quic_then_tcp_only_for_enabled_family() {
        let candidate = signed_candidate(true);
        let ipv4 = DialFamilies::from_listen(&ListenConfig::Ipv4 {
            ip: Ipv4Addr::UNSPECIFIED,
            port: 9000,
        })
        .retain(candidate.addresses());
        assert_eq!(
            ipv4,
            vec![
                "/ip4/127.0.0.1/udp/9001/quic-v1".parse::<Multiaddr>().unwrap(),
                "/ip4/127.0.0.1/tcp/9000".parse::<Multiaddr>().unwrap(),
            ]
        );
        let ipv6 = DialFamilies::from_listen(&ListenConfig::Ipv6 {
            ip: Ipv6Addr::UNSPECIFIED,
            port: 9000,
        })
        .retain(candidate.addresses());
        assert_eq!(
            ipv6,
            vec![
                "/ip6/::1/udp/9001/quic-v1".parse::<Multiaddr>().unwrap(),
                "/ip6/::1/tcp/9000".parse::<Multiaddr>().unwrap(),
            ]
        );
        let both = DialFamilies::from_listen(&ListenConfig::DualStack {
            ipv4: Ipv4Addr::UNSPECIFIED,
            ipv4_port: 9000,
            ipv6: Ipv6Addr::UNSPECIFIED,
            ipv6_port: 9000,
        })
        .retain(candidate.addresses());
        assert_eq!(both, candidate.addresses());
    }

    #[test]
    fn signed_ipv6_only_candidate_has_no_dial_endpoints_on_ipv4_node() {
        let candidate = signed_candidate(false);
        assert_eq!(candidate.addresses().len(), 2);
        let families = DialFamilies::from_listen(&ListenConfig::Ipv4 {
            ip: Ipv4Addr::UNSPECIFIED,
            port: 9000,
        });
        assert!(families.retain(candidate.addresses()).is_empty());
    }
}
