use std::net::IpAddr;
use std::str::FromStr;

use axum::http::HeaderMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustedProxyCidr {
    network: IpAddr,
    prefix: u8,
}

impl TrustedProxyCidr {
    pub fn contains(self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

impl FromStr for TrustedProxyCidr {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = value
            .split_once('/')
            .ok_or_else(|| format!("trusted proxy CIDR '{value}' must include a prefix"))?;
        let network: IpAddr = address
            .parse()
            .map_err(|error| format!("invalid trusted proxy address '{address}': {error}"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|error| format!("invalid trusted proxy prefix '{prefix}': {error}"))?;
        let max = if network.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return Err(format!("trusted proxy prefix {prefix} exceeds {max} for '{value}'"));
        }
        Ok(Self { network, prefix })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClientIdentityResolver {
    trusted_proxies: Vec<TrustedProxyCidr>,
}

impl ClientIdentityResolver {
    pub fn new(trusted_proxies: Vec<TrustedProxyCidr>) -> Self {
        Self { trusted_proxies }
    }

    pub fn resolve(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        if !self.is_trusted(peer) {
            return peer;
        }

        let chain = match forwarded_chain(headers) {
            Ok(Some(chain)) => chain,
            Ok(None) => return peer,
            Err(()) => return peer,
        };
        chain
            .into_iter()
            .rev()
            .find(|address| !self.is_trusted(*address))
            .unwrap_or(peer)
    }

    fn is_trusted(&self, address: IpAddr) -> bool {
        self.trusted_proxies
            .iter()
            .any(|network| network.contains(address))
    }
}

fn forwarded_chain(headers: &HeaderMap) -> Result<Option<Vec<IpAddr>>, ()> {
    let forwarded = headers.get_all(axum::http::header::FORWARDED);
    if forwarded.iter().next().is_some() {
        let mut chain = Vec::new();
        for value in forwarded.iter() {
            let value = value.to_str().map_err(|_| ())?;
            for element in value.split(',') {
                let raw = element
                    .split(';')
                    .find_map(|part| part.trim().strip_prefix("for="))
                    .ok_or(())?;
                chain.push(parse_forwarded_address(raw)?);
            }
        }
        return (!chain.is_empty())
            .then_some(chain)
            .ok_or(())
            .map(Some);
    }

    let forwarded_for = headers.get_all("x-forwarded-for");
    if forwarded_for.iter().next().is_none() {
        return Ok(None);
    }
    let mut chain = Vec::new();
    for value in forwarded_for.iter() {
        let value = value.to_str().map_err(|_| ())?;
        chain.extend(
            value
                .split(',')
                .map(|address| address.trim().parse::<IpAddr>().map_err(|_| ()))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    (!chain.is_empty())
        .then_some(chain)
        .ok_or(())
        .map(Some)
}

fn parse_forwarded_address(raw: &str) -> Result<IpAddr, ()> {
    let raw = raw.trim_matches('"');
    if let Some(ipv6) = raw
        .strip_prefix('[')
        .and_then(|value| value.split(']').next())
    {
        return ipv6.parse().map_err(|_| ());
    }
    raw.parse::<IpAddr>().or_else(|_| {
        raw.rsplit_once(':')
            .ok_or(())
            .and_then(|(address, _)| address.parse().map_err(|_| ()))
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn resolver() -> ClientIdentityResolver {
        ClientIdentityResolver::new(vec!["10.0.0.0/8".parse().unwrap(), "192.168.0.0/16".parse().unwrap()])
    }

    #[test]
    fn untrusted_peer_cannot_spoof_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.8".parse().unwrap());
        let peer = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4));
        assert_eq!(resolver().resolve(peer, &headers), peer);
    }

    #[test]
    fn trusted_chain_selects_nearest_untrusted_client() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.8, 192.168.1.4".parse().unwrap());
        let peer = "10.1.2.3".parse().unwrap();
        assert_eq!(
            resolver().resolve(peer, &headers),
            "203.0.113.8".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn repeated_x_forwarded_for_values_preserve_proxy_append_order() {
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", "1.2.3.4".parse().unwrap());
        headers.append("x-forwarded-for", "198.51.100.9".parse().unwrap());
        let peer = "10.1.2.3".parse().unwrap();
        assert_eq!(
            resolver().resolve(peer, &headers),
            "198.51.100.9".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn repeated_forwarded_values_preserve_proxy_append_order() {
        let mut headers = HeaderMap::new();
        headers.append(axum::http::header::FORWARDED, "for=1.2.3.4".parse().unwrap());
        headers.append(
            axum::http::header::FORWARDED,
            "for=198.51.100.9;proto=https".parse().unwrap(),
        );
        let peer = "10.1.2.3".parse().unwrap();
        assert_eq!(
            resolver().resolve(peer, &headers),
            "198.51.100.9".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn malformed_chain_falls_back_to_socket_peer() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "not-an-address".parse().unwrap());
        let peer = "10.1.2.3".parse().unwrap();
        assert_eq!(resolver().resolve(peer, &headers), peer);
    }

    #[test]
    fn cidr_prefix_is_validated() {
        assert!("10.0.0.0/33".parse::<TrustedProxyCidr>().is_err());
        assert!(
            "2001:db8::/129"
                .parse::<TrustedProxyCidr>()
                .is_err()
        );
    }
}
