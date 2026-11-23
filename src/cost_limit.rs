use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use log::{info, warn};
use sequence_trie::SequenceTrie;

use crate::client_net::{parse_client_ip, Cidr, Network};
use crate::config::CostLimitConfig;

/// The resources a network's budget is measured in, in the order of `CostLimiter::budgets`
pub const RESOURCES: [&str; 2] = ["backend_seconds", "response_bytes"];

/// Whether a request from a client may go to the backend
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Admission {
    /// The client is not subject to the limiter: allowlisted, or its address does not parse
    Exempt,
    /// The client's network is within budget; charge the cost of a cache miss to it
    Allowed(Network),
    /// The client's network is over budget until `retry_after` has passed
    Throttled {
        network: Network,
        retry_after: Duration,
    },
}

/// A network's spent cost per resource, drained linearly at budget/window since `updated`
struct Spent {
    amounts: [f64; RESOURCES.len()],
    updated: Instant,
    throttled: bool,
}

/// Limits each client network to a budget of backend cost of cache misses per window.
///
/// Each network's spent cost drains continuously at budget/window, so the budget behaves as a
/// sliding window that a network may spend in one burst.  A network whose spent cost exceeds the
/// budget in any resource is throttled until it has drained back below it.  The spent cost is
/// capped at twice the budget, so even a single very expensive request throttles a network for
/// at most one window.  Cost is charged after the backend answers, so requests that are already
/// in flight when a network crosses its budget still complete.
pub struct CostLimiter {
    window: Duration,
    budgets: [Option<f64>; RESOURCES.len()],
    allowlist: Vec<Cidr>,
    method_weights: SequenceTrie<String, f64>,
    networks: Mutex<HashMap<Network, Spent>>,
}

/// A network's spent cost as a fraction of its budget, for a resource that has one
pub struct UsageRatio {
    pub network: Network,
    pub resource: &'static str,
    pub ratio: f64,
}

impl CostLimiter {
    /// Builds the limiter from its config section.  Returns `Ok(None)` when it is disabled, and
    /// `Err` with a description when the section is invalid.
    pub fn from_config(config: &CostLimitConfig) -> Result<Option<CostLimiter>, String> {
        if !config.enabled {
            return Ok(None);
        }
        if config.window_seconds == 0 {
            return Err("cost_limit.window_seconds must be greater than 0".to_string());
        }
        let budgets = [
            config.backend_seconds,
            config.response_bytes.map(|bytes| bytes as f64),
        ];
        if budgets.iter().all(Option::is_none) {
            return Err(
                "cost_limit is enabled but neither backend_seconds nor response_bytes is set"
                    .to_string(),
            );
        }
        for (resource, budget) in RESOURCES.iter().zip(budgets) {
            if budget.is_some_and(|budget| !(budget.is_finite() && budget > 0.0)) {
                return Err(format!("cost_limit.{resource} must be a positive number"));
            }
        }
        let allowlist = config
            .allowlist
            .iter()
            .map(|entry| Cidr::parse(entry))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("cost_limit.allowlist: {e}"))?;
        let mut method_weights = SequenceTrie::new();
        for (method, weight) in &config.method_weights {
            if !(weight.is_finite() && *weight >= 0.0) {
                return Err(format!(
                    "cost_limit.method_weights.{method} must be a non-negative number"
                ));
            }
            let parts: Vec<String> = method.split('.').map(|v| v.to_string()).collect();
            method_weights.insert_owned(parts, *weight);
        }
        Ok(Some(CostLimiter {
            window: Duration::from_secs(config.window_seconds),
            budgets,
            allowlist,
            method_weights,
            networks: Mutex::new(HashMap::new()),
        }))
    }

    /// Decides whether a cache-missing request from `client_ip` may go to the backend at `now`
    pub fn admit(&self, client_ip: &str, now: Instant) -> Admission {
        let ip = match parse_client_ip(client_ip) {
            Some(ip) => ip,
            None => return Admission::Exempt,
        };
        if self.allowlist.iter().any(|cidr| cidr.contains(ip)) {
            return Admission::Exempt;
        }
        let network = Network::of(ip);
        let mut networks = self.networks.lock().unwrap_or_else(PoisonError::into_inner);
        let spent = match networks.get_mut(&network) {
            Some(spent) => spent,
            None => return Admission::Allowed(network),
        };
        let amounts = self.drained(spent, now);
        match self.time_until_within_budget(&amounts) {
            Some(retry_after) => {
                if !spent.throttled {
                    spent.throttled = true;
                    warn!(
                        "cost_limit: throttling cache misses from {network}: {}",
                        self.describe(&amounts)
                    );
                }
                Admission::Throttled {
                    network,
                    retry_after,
                }
            }
            None => {
                if spent.throttled {
                    spent.throttled = false;
                    info!("cost_limit: {network} is within its budget again");
                }
                Admission::Allowed(network)
            }
        }
    }

    /// Charges the backend cost of a cache miss for `method_name_parts` to `network`
    pub fn charge(
        &self,
        network: Network,
        method_name_parts: Vec<&String>,
        backend_seconds: f64,
        response_bytes: u64,
        now: Instant,
    ) {
        let weight = *self
            .method_weights
            .get_ancestor(method_name_parts)
            .unwrap_or(&1.0);
        let costs = [backend_seconds * weight, response_bytes as f64 * weight];
        let mut networks = self.networks.lock().unwrap_or_else(PoisonError::into_inner);
        let spent = networks.entry(network).or_insert_with(|| Spent {
            amounts: [0.0; RESOURCES.len()],
            updated: now,
            throttled: false,
        });
        self.drain(spent, now);
        for ((amount, cost), budget) in spent.amounts.iter_mut().zip(costs).zip(self.budgets) {
            if let Some(budget) = budget {
                *amount = (*amount + cost).min(2.0 * budget);
            }
        }
    }

    /// Reports each tracked network's spent cost per budgeted resource, and forgets the networks
    /// that have drained to zero
    pub fn usage_ratios(&self, now: Instant) -> Vec<UsageRatio> {
        let mut networks = self.networks.lock().unwrap_or_else(PoisonError::into_inner);
        let mut ratios = Vec::new();
        networks.retain(|network, spent| {
            self.drain(spent, now);
            for ((resource, amount), budget) in
                RESOURCES.iter().zip(spent.amounts).zip(self.budgets)
            {
                if let Some(budget) = budget {
                    ratios.push(UsageRatio {
                        network: *network,
                        resource,
                        ratio: amount / budget,
                    });
                }
            }
            spent.amounts.iter().any(|amount| *amount > 0.0)
        });
        ratios
    }

    fn drained(&self, spent: &Spent, now: Instant) -> [f64; RESOURCES.len()] {
        let elapsed = now.saturating_duration_since(spent.updated).as_secs_f64();
        let window = self.window.as_secs_f64();
        let mut amounts = spent.amounts;
        for (amount, budget) in amounts.iter_mut().zip(self.budgets) {
            if let Some(budget) = budget {
                *amount = (*amount - budget * elapsed / window).max(0.0);
            }
        }
        amounts
    }

    fn drain(&self, spent: &mut Spent, now: Instant) {
        spent.amounts = self.drained(spent, now);
        spent.updated = spent.updated.max(now);
    }

    fn time_until_within_budget(&self, amounts: &[f64; RESOURCES.len()]) -> Option<Duration> {
        let window = self.window.as_secs_f64();
        amounts
            .iter()
            .zip(self.budgets)
            .filter_map(|(amount, budget)| {
                budget
                    .filter(|budget| amount > budget)
                    .map(|budget| (amount - budget) * window / budget)
            })
            .reduce(f64::max)
            .map(Duration::from_secs_f64)
    }

    fn describe(&self, amounts: &[f64; RESOURCES.len()]) -> String {
        RESOURCES
            .iter()
            .zip(*amounts)
            .zip(self.budgets)
            .filter_map(|((resource, amount), budget)| {
                budget.map(|budget| {
                    format!(
                        "{resource} {amount:.1}/{budget} per {}s",
                        self.window.as_secs()
                    )
                })
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, CostLimiter, Network, RESOURCES};
    use crate::config::CostLimitConfig;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    fn limiter(
        backend_seconds: Option<f64>,
        response_bytes: Option<u64>,
        allowlist: &[&str],
    ) -> CostLimiter {
        CostLimiter::from_config(&CostLimitConfig {
            enabled: true,
            window_seconds: 60,
            backend_seconds,
            response_bytes,
            allowlist: allowlist.iter().map(|entry| entry.to_string()).collect(),
            method_weights: HashMap::new(),
        })
        .unwrap()
        .unwrap()
    }

    fn parts(dotted: &str) -> Vec<String> {
        dotted.split('.').map(|v| v.to_string()).collect()
    }

    fn charge(
        limiter: &CostLimiter,
        client_ip: &str,
        method: &str,
        backend_seconds: f64,
        response_bytes: u64,
        now: Instant,
    ) {
        let network = match limiter.admit(client_ip, now) {
            Admission::Allowed(network) | Admission::Throttled { network, .. } => network,
            Admission::Exempt => panic!("{client_ip} is exempt"),
        };
        let method_parts = parts(method);
        limiter.charge(
            network,
            method_parts.iter().collect(),
            backend_seconds,
            response_bytes,
            now,
        );
    }

    fn network_of(limiter: &CostLimiter, client_ip: &str) -> String {
        match limiter.admit(client_ip, Instant::now()) {
            Admission::Allowed(network) => network.to_string(),
            other => panic!("{client_ip} was not admitted: {other:?}"),
        }
    }

    #[test]
    fn ipv4_clients_are_keyed_by_slash_24() {
        let limiter = limiter(Some(1.0), None, &[]);
        assert_eq!(network_of(&limiter, "203.0.113.77"), "203.0.113.0/24");
        assert_eq!(
            network_of(&limiter, " 203.0.113.200:4431 "),
            "203.0.113.0/24"
        );
        assert_eq!(network_of(&limiter, "::ffff:203.0.113.5"), "203.0.113.0/24");
        assert_ne!(network_of(&limiter, "203.0.114.77"), "203.0.113.0/24");
    }

    #[test]
    fn ipv6_clients_are_keyed_by_slash_64() {
        let limiter = limiter(Some(1.0), None, &[]);
        assert_eq!(
            network_of(&limiter, "2001:db8:1:2:aaaa:bbbb:cccc:dddd"),
            "2001:db8:1:2::/64"
        );
        assert_eq!(
            network_of(&limiter, "[2001:db8:1:2::9]:443"),
            "2001:db8:1:2::/64"
        );
        assert_eq!(network_of(&limiter, "2001:db8:1:3::1"), "2001:db8:1:3::/64");
    }

    #[test]
    fn one_address_spends_the_budget_of_its_whole_network() {
        let limiter = limiter(Some(1.0), None, &[]);
        let now = Instant::now();
        charge(
            &limiter,
            "203.0.113.1",
            "appbase.condenser_api.get_accounts",
            0.6,
            0,
            now,
        );
        charge(
            &limiter,
            "203.0.113.2",
            "appbase.condenser_api.get_accounts",
            0.6,
            0,
            now,
        );
        assert!(matches!(
            limiter.admit("203.0.113.3", now),
            Admission::Throttled { .. }
        ));
        assert!(matches!(
            limiter.admit("203.0.114.3", now),
            Admission::Allowed(_)
        ));
        assert!(matches!(
            limiter.admit("2001:db8::1", now),
            Admission::Allowed(_)
        ));
    }

    #[test]
    fn unparseable_addresses_are_exempt() {
        let limiter = limiter(Some(1.0), None, &[]);
        assert_eq!(
            limiter.admit("not-an-ip", Instant::now()),
            Admission::Exempt
        );
    }

    #[test]
    fn spent_cost_drains_over_the_sliding_window() {
        let limiter = limiter(Some(10.0), None, &[]);
        let start = Instant::now();
        let network = Network::of("198.51.100.9".parse().unwrap());

        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            10.0,
            0,
            start,
        );
        assert_eq!(
            limiter.admit("198.51.100.9", start),
            Admission::Allowed(network),
            "spending exactly the budget is allowed"
        );

        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            5.0,
            0,
            start,
        );
        match limiter.admit("198.51.100.9", start) {
            Admission::Throttled { retry_after, .. } => {
                assert_eq!(retry_after, Duration::from_secs(30))
            }
            other => panic!("expected throttled, got {other:?}"),
        }
        assert!(matches!(
            limiter.admit("198.51.100.9", start + Duration::from_secs(29)),
            Admission::Throttled { .. }
        ));
        assert_eq!(
            limiter.admit("198.51.100.9", start + Duration::from_secs(30)),
            Admission::Allowed(network)
        );
    }

    #[test]
    fn a_huge_request_throttles_for_at_most_one_window() {
        let limiter = limiter(None, Some(1_000_000), &[]);
        let start = Instant::now();
        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            0.0,
            13_800_000,
            start,
        );
        match limiter.admit("198.51.100.9", start) {
            Admission::Throttled { retry_after, .. } => {
                assert_eq!(retry_after, Duration::from_secs(60))
            }
            other => panic!("expected throttled, got {other:?}"),
        }
        assert!(matches!(
            limiter.admit("198.51.100.9", start + Duration::from_secs(59)),
            Admission::Throttled { .. }
        ));
        assert!(matches!(
            limiter.admit("198.51.100.9", start + Duration::from_secs(60)),
            Admission::Allowed(_)
        ));
    }

    #[test]
    fn any_exceeded_resource_throttles() {
        let limiter = limiter(Some(100.0), Some(1000), &[]);
        let start = Instant::now();
        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            0.01,
            5000,
            start,
        );
        assert!(matches!(
            limiter.admit("198.51.100.9", start),
            Admission::Throttled { .. }
        ));
    }

    #[test]
    fn allowlisted_networks_are_never_throttled() {
        let limiter = limiter(
            Some(1.0),
            None,
            &["10.0.0.0/8", "192.0.2.10", "2001:db8::/32"],
        );
        for client_ip in [
            "10.20.30.40",
            "192.0.2.10",
            "2001:db8:ffff::1",
            "::ffff:10.0.0.1",
        ] {
            assert_eq!(
                limiter.admit(client_ip, Instant::now()),
                Admission::Exempt,
                "{client_ip}"
            );
        }
        assert!(matches!(
            limiter.admit("192.0.2.11", Instant::now()),
            Admission::Allowed(_)
        ));
        assert!(matches!(
            limiter.admit("11.0.0.1", Instant::now()),
            Admission::Allowed(_)
        ));
    }

    #[test]
    fn method_weights_scale_the_cost_by_the_most_specific_match() {
        let mut config = CostLimitConfig {
            enabled: true,
            window_seconds: 60,
            backend_seconds: Some(10.0),
            ..CostLimitConfig::default()
        };
        config.method_weights.insert("hive".to_string(), 0.0);
        config
            .method_weights
            .insert("hive.get_account_history".to_string(), 5.0);
        let limiter = CostLimiter::from_config(&config).unwrap().unwrap();
        let start = Instant::now();

        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_accounts",
            100.0,
            0,
            start,
        );
        assert!(matches!(
            limiter.admit("198.51.100.9", start),
            Admission::Allowed(_)
        ));
        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            3.0,
            0,
            start,
        );
        assert!(matches!(
            limiter.admit("198.51.100.9", start),
            Admission::Throttled { .. }
        ));
    }

    #[test]
    fn usage_ratios_report_budget_use_and_forget_drained_networks() {
        let limiter = limiter(Some(10.0), Some(1000), &[]);
        let start = Instant::now();
        charge(
            &limiter,
            "198.51.100.9",
            "hive.get_account_history",
            5.0,
            250,
            start,
        );

        let ratios = limiter.usage_ratios(start);
        assert_eq!(ratios.len(), RESOURCES.len());
        assert!(ratios
            .iter()
            .all(|ratio| ratio.network.to_string() == "198.51.100.0/24"));
        let ratio_of = |resource: &str| {
            ratios
                .iter()
                .find(|ratio| ratio.resource == resource)
                .unwrap()
                .ratio
        };
        assert_eq!(ratio_of("backend_seconds"), 0.5);
        assert_eq!(ratio_of("response_bytes"), 0.25);

        limiter.usage_ratios(start + Duration::from_secs(60));
        assert!(limiter
            .usage_ratios(start + Duration::from_secs(61))
            .is_empty());
    }

    #[test]
    fn config_is_validated() {
        let enabled = |config: CostLimitConfig| {
            CostLimiter::from_config(&CostLimitConfig {
                enabled: true,
                ..config
            })
            .map(|limiter| limiter.is_some())
        };
        assert_eq!(
            CostLimiter::from_config(&CostLimitConfig {
                backend_seconds: Some(1.0),
                ..CostLimitConfig::default()
            })
            .map(|limiter| limiter.is_some()),
            Ok(false)
        );
        assert!(
            enabled(CostLimitConfig {
                window_seconds: 60,
                ..CostLimitConfig::default()
            })
            .is_err(),
            "no budget"
        );
        assert!(enabled(CostLimitConfig {
            window_seconds: 0,
            backend_seconds: Some(1.0),
            ..CostLimitConfig::default()
        })
        .is_err());
        assert!(enabled(CostLimitConfig {
            window_seconds: 60,
            backend_seconds: Some(-1.0),
            ..CostLimitConfig::default()
        })
        .is_err());
        assert!(enabled(CostLimitConfig {
            window_seconds: 60,
            backend_seconds: Some(1.0),
            allowlist: vec!["10.0.0.0/33".to_string()],
            ..CostLimitConfig::default()
        })
        .is_err());
        assert!(enabled(CostLimitConfig {
            window_seconds: 60,
            backend_seconds: Some(1.0),
            allowlist: vec!["nope".to_string()],
            ..CostLimitConfig::default()
        })
        .is_err());
        assert_eq!(
            enabled(CostLimitConfig {
                window_seconds: 60,
                response_bytes: Some(1),
                ..CostLimitConfig::default()
            }),
            Ok(true)
        );
    }
}
