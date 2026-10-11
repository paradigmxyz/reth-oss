//! Signature-verified discovery routing seeds, distinct from current consensus peers.
//!
//! A bootstrap ENR proves a routing identity and permitted UDP endpoint only. Neither a
//! missing nor historical `eth2` field grants fork, transport, status or consensus authority.

use super::{DiscoveryError, MAX_ENR_TEXT};
use crate::EndpointPolicy;
use discv5::{
    enr::{CombinedPublicKey, NodeId},
    Enr,
};
use std::net::IpAddr;

/// A bounded signed routing record with no peer addresses or consensus compatibility token.
#[derive(Clone, Debug)]
pub struct RoutingSeedRecord {
    pub(super) enr: Enr,
}

impl RoutingSeedRecord {
    /// Authenticate the original signature, secp256k1 identity and every advertised UDP route.
    /// The configured network never derives trust from this record's optional fork fields.
    pub fn parse(text: &str, policy: EndpointPolicy) -> Result<Self, DiscoveryError> {
        if text.len() > MAX_ENR_TEXT || !text.starts_with("enr:") {
            return Err(DiscoveryError::Record);
        }
        let enr = text.parse::<Enr>().map_err(|_| DiscoveryError::Record)?;
        verify(&enr, policy)?;
        Ok(Self { enr })
    }

    /// Discovery routing identity only, never an authenticated consensus peer identity.
    pub const fn node_id(&self) -> NodeId {
        self.enr.node_id()
    }
}

fn verify(enr: &Enr, policy: EndpointPolicy) -> Result<(), DiscoveryError> {
    if enr.to_base64().len() > MAX_ENR_TEXT ||
        !enr.verify() ||
        !matches!(enr.public_key(), CombinedPublicKey::Secp256k1(_))
    {
        return Err(DiscoveryError::Record);
    }
    let mut route = false;
    for (ip_key, ip, udp_key) in
        [("ip", enr.ip4().map(IpAddr::V4), "udp"), ("ip6", enr.ip6().map(IpAddr::V6), "udp6")]
    {
        if enr.get_raw_rlp(ip_key).is_some() && ip.is_none() {
            return Err(DiscoveryError::Record);
        }
        if let Some(ip) = ip &&
            !policy.permits(ip)
        {
            return Err(DiscoveryError::Endpoint);
        }
        if let Some(port) = enr.get_decodable::<u16>(udp_key) {
            let port = port.map_err(|_| DiscoveryError::Record)?;
            if ip.is_none() || port == 0 {
                return Err(DiscoveryError::Endpoint);
            }
            route = true;
        }
    }
    if !route {
        return Err(DiscoveryError::Endpoint);
    }
    Ok(())
}

/// The upstream routing filter is a function pointer; choose the actual owner's endpoint policy.
/// FINDNODE completion and all candidate events still use strict `DiscoveryRecord::verify`.
pub(super) fn table_filter(policy: EndpointPolicy) -> fn(&Enr) -> bool {
    match policy {
        EndpointPolicy::Public => public_route,
        EndpointPolicy::PrivateNetwork => private_route,
        EndpointPolicy::LoopbackOnly => loopback_route,
    }
}

fn public_route(enr: &Enr) -> bool {
    verify(enr, EndpointPolicy::Public).is_ok()
}

fn private_route(enr: &Enr) -> bool {
    verify(enr, EndpointPolicy::PrivateNetwork).is_ok()
}

fn loopback_route(enr: &Enr) -> bool {
    verify(enr, EndpointPolicy::LoopbackOnly).is_ok()
}

#[cfg(test)]
mod tests;
