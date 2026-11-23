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
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

fn prefix_mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    }
}

fn prefix_mask_v6(prefix: u8) -> u128 {
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

/// A network in CIDR notation, `a.b.c.d/n` or `x::y/n`, or a single address
#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(text: &str) -> Result<Cidr, String> {
        let (addr_text, prefix_text) = match text.split_once('/') {
            Some((addr_text, prefix_text)) => (addr_text, Some(prefix_text)),
            None => (text, None),
        };
        let addr = canonical(
            addr_text
                .trim()
                .parse::<IpAddr>()
                .map_err(|e| format!("invalid address \"{text}\": {e}"))?,
        );
        let max_prefix = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix_text {
            Some(prefix_text) => prefix_text
                .trim()
                .parse::<u8>()
                .ok()
                .filter(|prefix| *prefix <= max_prefix)
                .ok_or_else(|| format!("invalid prefix length in \"{text}\""))?,
            None => max_prefix,
        };
        Ok(Cidr { addr, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = prefix_mask_v4(self.prefix);
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = prefix_mask_v6(self.prefix);
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

/// The reverse proxies in front of drone whose `X-Forwarded-For` entries are believed.  Each
/// proxy appends the address it received the request from, so only the right-hand end of the
/// header is trustworthy: anything left of the first address that is not a trusted proxy was
/// written by the client and may be spoofed.
#[derive(Debug, Clone)]
pub struct TrustedProxies {
    proxies: Vec<Cidr>,
}

impl TrustedProxies {
    /// Builds the proxy set from CIDRs or single addresses.  An empty list trusts only the peer
    /// drone accepted the connection from, i.e. a single proxy hop.
    pub fn from_config(entries: &[String]) -> Result<TrustedProxies, String> {
        let proxies = entries
            .iter()
            .map(|entry| Cidr::parse(entry))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TrustedProxies { proxies })
    }

    fn trusts(&self, ip: IpAddr) -> bool {
        self.proxies.iter().any(|cidr| cidr.contains(ip))
    }

    /// The client address of a request that reached drone from `peer` with `forwarded_for` as
    /// its `X-Forwarded-For`: walking from the peer leftwards through the header, the first
    /// address that is not a trusted proxy.  When the peer itself is not trusted the header is
    /// ignored.  When every hop is trusted, or the walk reaches an entry that is not an address,
    /// the last trusted hop is the client.
    pub fn client_of(&self, forwarded_for: Option<&str>, peer: IpAddr) -> IpAddr {
        let peer = canonical(peer);
        if !self.proxies.is_empty() && !self.trusts(peer) {
            return peer;
        }
        let mut client = peer;
        for entry in forwarded_for.unwrap_or("").rsplit(',') {
            match parse_client_ip(entry) {
                Some(ip) if self.trusts(ip) => client = ip,
                Some(ip) => return ip,
                None => break,
            }
        }
        client
    }
}

#[cfg(test)]
mod tests {
    use super::{Network, TrustedProxies};
    use std::net::IpAddr;

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

    fn client_of(trusted: &[&str], forwarded_for: Option<&str>, peer: &str) -> String {
        let trusted: Vec<String> = trusted.iter().map(|entry| entry.to_string()).collect();
        TrustedProxies::from_config(&trusted)
            .unwrap()
            .client_of(forwarded_for, peer.parse::<IpAddr>().unwrap())
            .to_string()
    }

    #[test]
    fn one_proxy_hop_takes_the_address_the_proxy_appended() {
        // the client sent `X-Forwarded-For: 1.2.3.4`; the proxy at 10.0.0.1 appended 203.0.113.7
        let spoofed = Some("1.2.3.4, 203.0.113.7");
        assert_eq!(client_of(&[], spoofed, "10.0.0.1"), "203.0.113.7");
        assert_eq!(client_of(&["10.0.0.1"], spoofed, "10.0.0.1"), "203.0.113.7");
        assert_eq!(client_of(&[], Some("203.0.113.7"), "10.0.0.1"), "203.0.113.7");
    }

    #[test]
    fn two_proxy_hops_skip_every_trusted_proxy() {
        // client 203.0.113.7 -> CDN edge 198.51.100.9 -> local proxy 10.0.0.1 -> drone
        let trusted = ["10.0.0.0/8", "198.51.100.0/24"];
        assert_eq!(
            client_of(&trusted, Some("203.0.113.7, 198.51.100.9"), "10.0.0.1"),
            "203.0.113.7"
        );
        assert_eq!(
            client_of(&trusted, Some("1.2.3.4, 203.0.113.7, 198.51.100.9"), "10.0.0.1"),
            "203.0.113.7"
        );
        assert_eq!(
            client_of(
                &["10.0.0.0/8", "2001:db8:ffff::/48"],
                Some("2001:db8:1:2::9, 2001:db8:ffff::1"),
                "::ffff:10.0.0.1"
            ),
            "2001:db8:1:2::9"
        );
    }

    #[test]
    fn untrusted_peers_and_trusted_only_chains_do_not_follow_client_entries() {
        let trusted = ["10.0.0.0/8"];
        // a client connecting directly cannot choose its address
        assert_eq!(client_of(&trusted, Some("1.2.3.4"), "203.0.113.7"), "203.0.113.7");
        // garbage left of the last trusted hop is not followed past
        assert_eq!(
            client_of(&trusted, Some("1.2.3.4, not-an-ip, 10.0.0.2"), "10.0.0.1"),
            "10.0.0.2"
        );
        assert_eq!(client_of(&trusted, Some("10.0.0.3, 10.0.0.2"), "10.0.0.1"), "10.0.0.3");
        assert_eq!(client_of(&[], None, "10.0.0.1"), "10.0.0.1");
        assert_eq!(client_of(&[], Some(""), "10.0.0.1"), "10.0.0.1");
    }

    #[test]
    fn invalid_trusted_proxies_are_rejected() {
        for entry in ["not-an-ip", "10.0.0.0/33", "2001:db8::/129"] {
            assert!(TrustedProxies::from_config(&[entry.to_string()]).is_err(), "{entry}");
        }
    }
}
