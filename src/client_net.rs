use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// A client network, the unit client cost is accounted and limited per: an IPv4 /24 or an IPv6 /64
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Network(IpAddr);

impl Network {
    pub fn of(ip: IpAddr) -> Network {
        match canonical(ip) {
            IpAddr::V4(v4) => Network(IpAddr::V4(Ipv4Addr::from(
                u32::from(v4) & prefix_mask_v4(24),
            ))),
            IpAddr::V6(v6) => Network(IpAddr::V6(Ipv6Addr::from(
                u128::from(v6) & prefix_mask_v6(64),
            ))),
        }
    }

    /// The network of a client address as resolved from `CF-Connecting-IP` / `X-Forwarded-For` /
    /// the peer; `None` when the address does not parse
    pub fn of_client(client_ip: &str) -> Option<Network> {
        parse_client_ip(client_ip).map(Network::of)
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            IpAddr::V4(v4) => write!(f, "{v4}/24"),
            IpAddr::V6(v6) => write!(f, "{v6}/64"),
        }
    }
}

/// IPv4 clients reaching a dual-stack socket appear as `::ffff:a.b.c.d`; key them as IPv4
pub fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

pub fn prefix_mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    }
}

pub fn prefix_mask_v6(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - u32::from(prefix))
    }
}

/// Parses a client address as resolved from `CF-Connecting-IP` / `X-Forwarded-For` / the peer,
/// tolerating a trailing port
pub fn parse_client_ip(client_ip: &str) -> Option<IpAddr> {
    let trimmed = client_ip.trim();
    trimmed
        .parse::<IpAddr>()
        .ok()
        .or_else(|| trimmed.parse::<SocketAddr>().ok().map(|socket| socket.ip()))
        .map(canonical)
}

#[cfg(test)]
mod tests {
    use super::Network;

    fn network_of(client_ip: &str) -> Option<String> {
        Network::of_client(client_ip).map(|network| network.to_string())
    }

    #[test]
    fn ipv4_clients_are_keyed_by_slash_24() {
        assert_eq!(
            network_of("203.0.113.77").as_deref(),
            Some("203.0.113.0/24")
        );
        assert_eq!(
            network_of(" 203.0.113.200:4431 ").as_deref(),
            Some("203.0.113.0/24")
        );
        assert_eq!(
            network_of("::ffff:203.0.113.5").as_deref(),
            Some("203.0.113.0/24")
        );
        assert_eq!(
            network_of("203.0.114.77").as_deref(),
            Some("203.0.114.0/24")
        );
    }

    #[test]
    fn ipv6_clients_are_keyed_by_slash_64() {
        assert_eq!(
            network_of("2001:db8:1:2:aaaa:bbbb:cccc:dddd").as_deref(),
            Some("2001:db8:1:2::/64")
        );
        assert_eq!(
            network_of("[2001:db8:1:2::9]:443").as_deref(),
            Some("2001:db8:1:2::/64")
        );
        assert_eq!(
            network_of("2001:db8:1:3::1").as_deref(),
            Some("2001:db8:1:3::/64")
        );
    }

    #[test]
    fn unparseable_addresses_have_no_network() {
        assert_eq!(network_of("not-an-ip"), None);
        assert_eq!(network_of(""), None);
    }
}
