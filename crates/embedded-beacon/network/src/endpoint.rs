//! Explicit address policy for dormant peer transports.

use std::net::IpAddr;

/// Explicit routing policy; loopback operation is confined to transport tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointPolicy {
    /// Only loopback endpoints are accepted, including the local discovery listener.
    LoopbackOnly,
    /// Public unicast endpoints are accepted; private, local and reserved ranges are denied.
    Public,
    /// Explicit isolated-network routing over loopback, private IPv4 or unique-local IPv6.
    /// Advertised public destinations, unspecified addresses and link-local scopes are forbidden.
    PrivateNetwork,
}

impl EndpointPolicy {
    /// Whether a local socket may bind an address independently of its advertisement.
    /// Public peers can bind a private interface behind explicitly configured address translation.
    /// These local addresses never become permitted remote dial targets or ENR advertisements.
    pub fn permits_bind(self, ip: IpAddr) -> bool {
        match self {
            Self::LoopbackOnly => ip.is_loopback(),
            Self::PrivateNetwork => ip.is_unspecified() || self.permits(ip),
            Self::Public => match ip {
                IpAddr::V4(ip) => {
                    ip.is_unspecified() ||
                        (ip.octets()[0] != 0 && ip.octets()[0] < 224 &&
                         !ip.is_multicast() && !ip.is_broadcast())
                }
                IpAddr::V6(ip) => !ip.is_multicast(),
            },
        }
    }

    /// Whether an advertised IP is permitted without DNS resolution.
    pub fn permits(self, ip: IpAddr) -> bool {
        match self {
            Self::LoopbackOnly => ip.is_loopback(),
            Self::PrivateNetwork => match ip {
                IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
                IpAddr::V6(ip) => ip.is_loopback() || ip.segments()[0] & 0xfe00 == 0xfc00,
            },
            Self::Public => match ip {
                IpAddr::V4(ip) => {
                    let [first, second, third, _] = ip.octets();
                    !ip.is_private() &&
                        !ip.is_loopback() &&
                        !ip.is_link_local() &&
                        !ip.is_multicast() &&
                        !ip.is_broadcast() &&
                        !ip.is_unspecified() &&
                        !ip.is_documentation() &&
                        first != 0 &&
                        first < 224 &&
                        !(first == 100 && (64..=127).contains(&second)) &&
                        !(first == 198 && (second == 18 || second == 19)) &&
                        !(first == 192 && second == 0 && third == 0)
                }
                IpAddr::V6(ip) => {
                    let segments = ip.segments();
                    // Public IPv6 global unicast. Reject mapped IPv4 and documentation ranges.
                    segments[0] & 0xe000 == 0x2000 &&
                        !(segments[0] == 0x2001 && segments[1] == 0x0db8)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_binding_never_relaxes_public_advertisement_or_remote_routing() {
        for text in ["0.0.0.0", "::", "192.168.1.2", "127.0.0.1", "fd00::1"] {
            let ip = text.parse().unwrap();
            assert!(EndpointPolicy::Public.permits_bind(ip));
            assert!(!EndpointPolicy::Public.permits(ip));
        }
        for text in ["224.0.0.1", "ff02::1", "255.255.255.255", "0.1.2.3"] {
            assert!(!EndpointPolicy::Public.permits_bind(text.parse().unwrap()));
        }
        for text in ["0.0.0.0", "::"] {
            let ip = text.parse().unwrap();
            assert!(EndpointPolicy::PrivateNetwork.permits_bind(ip));
            assert!(!EndpointPolicy::PrivateNetwork.permits(ip));
            assert!(!EndpointPolicy::LoopbackOnly.permits_bind(ip));
        }
    }

    #[test]
    fn isolated_literal_routes_never_expand_public_or_loopback_policies() {
        for text in
            ["10.0.0.1", "172.16.0.1", "172.31.255.254", "192.168.1.1", "fc00::1", "fdff::2"]
        {
            let ip = text.parse().unwrap();
            assert!(EndpointPolicy::PrivateNetwork.permits(ip));
            assert!(!EndpointPolicy::Public.permits(ip));
            assert!(!EndpointPolicy::LoopbackOnly.permits(ip));
        }
        for text in [
            "0.0.0.0",
            "::",
            "169.254.1.1",
            "fe80::1",
            "224.0.0.1",
            "ff02::1",
            "255.255.255.255",
            "8.8.8.8",
            "2001:4860:4860::8888",
        ] {
            assert!(!EndpointPolicy::PrivateNetwork.permits(text.parse().unwrap()));
        }
        for text in ["127.0.0.1", "::1"] {
            let ip = text.parse().unwrap();
            assert!(EndpointPolicy::PrivateNetwork.permits(ip));
            assert!(EndpointPolicy::LoopbackOnly.permits(ip));
            assert!(!EndpointPolicy::Public.permits(ip));
        }
    }
}
