use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::client_net::Network;
use crate::config::DroneConfig;

/// Networks past this many are not tracked individually but counted as `other` straight away,
/// so a client spread over many networks (e.g. IPv6 /64s) cannot grow the table without bound
const MAX_TRACKED_NETWORKS: usize = 100_000;

/// A network outside the top whose recent backend seconds have decayed below this is forgotten,
/// its totals moving to `other`
const FORGET_BELOW_BACKEND_SECONDS: f64 = 0.001;

/// What a client network (or `other`) has cost since drone started
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Totals {
    pub backend_seconds: f64,
    pub response_bytes: u64,
    pub requests: u64,
}

impl Totals {
    fn add(&mut self, other: &Totals) {
        self.backend_seconds += other.backend_seconds;
        self.response_bytes += other.response_bytes;
        self.requests += other.requests;
    }
}

struct Tracked {
    totals: Totals,
    /// backend seconds, decaying exponentially with the window as time constant since `updated`
    recent_backend_seconds: f64,
    updated: Instant,
}

#[derive(Default)]
struct State {
    networks: HashMap<Network, Tracked>,
    /// the totals of the networks that are not tracked individually
    untracked: Totals,
}

/// The networks to export individually, by recent backend seconds descending, and the rest
#[derive(Debug, PartialEq)]
pub struct TopNetworks {
    pub networks: Vec<(Network, Totals)>,
    pub other: Totals,
}

/// Accounts the cost of every request to its client network, and picks the networks that spent
/// the most backend seconds recently, so they can be exported with a bounded number of labels.
///
/// The sum of the exported totals (top networks plus `other`) only grows; a single network's
/// totals move into `other` when it drops out of the top and is forgotten, and `other` shrinks
/// by a network's totals when that network enters the top.
pub struct ClientCostTracker {
    top_networks: usize,
    window: Duration,
    state: Mutex<State>,
}

impl ClientCostTracker {
    /// Builds the tracker from the `drone` config section.  Returns `Ok(None)` when client cost
    /// metrics are off (or metrics as a whole are), and `Err` with a description when the
    /// settings are invalid.
    pub fn from_config(config: &DroneConfig) -> Result<Option<ClientCostTracker>, String> {
        if !(config.client_cost_metrics && config.metrics_enabled) {
            return Ok(None);
        }
        if config.client_cost_window_seconds == 0 {
            return Err("drone.client_cost_window_seconds must be greater than 0".to_string());
        }
        Ok(Some(ClientCostTracker::new(
            config.client_cost_top_networks,
            Duration::from_secs(config.client_cost_window_seconds),
        )))
    }

    fn new(top_networks: usize, window: Duration) -> ClientCostTracker {
        ClientCostTracker {
            top_networks,
            window,
            state: Mutex::new(State::default()),
        }
    }

    /// Charges one request to `network` (`None`: the client address did not parse)
    pub fn record(
        &self,
        network: Option<Network>,
        backend_seconds: f64,
        response_bytes: u64,
        now: Instant,
    ) {
        let cost = Totals {
            backend_seconds,
            response_bytes,
            requests: 1,
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let state = &mut *state;
        let tracked = match network {
            Some(network)
                if state.networks.len() < MAX_TRACKED_NETWORKS
                    || state.networks.contains_key(&network) =>
            {
                state.networks.entry(network).or_insert_with(|| Tracked {
                    totals: Totals::default(),
                    recent_backend_seconds: 0.0,
                    updated: now,
                })
            }
            _ => {
                state.untracked.add(&cost);
                return;
            }
        };
        tracked.recent_backend_seconds = self.decayed(tracked, now) + backend_seconds;
        tracked.updated = tracked.updated.max(now);
        tracked.totals.add(&cost);
    }

    /// Picks the networks with the most recent backend seconds at `now`, at most `top_networks`
    /// of them and none that spent nothing recently, and forgets the networks outside them that
    /// have gone quiet
    pub fn top(&self, now: Instant) -> TopNetworks {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let state = &mut *state;
        let mut ranked: Vec<(Network, f64)> = state
            .networks
            .iter()
            .map(|(network, tracked)| (*network, self.decayed(tracked, now)))
            .filter(|(_, recent)| *recent > 0.0)
            .collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
        ranked.truncate(self.top_networks);

        let mut other = state.untracked;
        let mut forgotten = Totals::default();
        state.networks.retain(|network, tracked| {
            if ranked.iter().any(|(top, _)| top == network) {
                return true;
            }
            other.add(&tracked.totals);
            if self.decayed(tracked, now) < FORGET_BELOW_BACKEND_SECONDS {
                forgotten.add(&tracked.totals);
                return false;
            }
            true
        });
        state.untracked.add(&forgotten);

        let networks = ranked
            .iter()
            .map(|(network, _)| (*network, state.networks[network].totals))
            .collect();
        TopNetworks { networks, other }
    }

    fn decayed(&self, tracked: &Tracked, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(tracked.updated).as_secs_f64();
        tracked.recent_backend_seconds * (-elapsed / self.window.as_secs_f64()).exp()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientCostTracker, Totals};
    use crate::client_net::Network;
    use std::time::{Duration, Instant};

    fn net(client_ip: &str) -> Option<Network> {
        Network::of_client(client_ip)
    }

    fn labels(tracker: &ClientCostTracker, now: Instant) -> Vec<String> {
        tracker
            .top(now)
            .networks
            .iter()
            .map(|(network, _)| network.to_string())
            .collect()
    }

    #[test]
    fn top_networks_are_ranked_by_backend_seconds_and_the_rest_is_other() {
        let tracker = ClientCostTracker::new(2, Duration::from_secs(300));
        let now = Instant::now();
        tracker.record(net("203.0.113.1"), 3.0, 1000, now);
        tracker.record(net("203.0.113.2"), 2.0, 500, now);
        tracker.record(net("198.51.100.1"), 4.0, 10, now);
        tracker.record(net("2001:db8::1"), 1.0, 7, now);
        tracker.record(net("192.0.2.1"), 0.0, 99, now);
        tracker.record(None, 0.5, 1, now);

        let top = tracker.top(now);
        assert_eq!(
            top.networks
                .iter()
                .map(|(network, _)| network.to_string())
                .collect::<Vec<_>>(),
            ["203.0.113.0/24", "198.51.100.0/24"]
        );
        assert_eq!(
            top.networks[0].1,
            Totals {
                backend_seconds: 5.0,
                response_bytes: 1500,
                requests: 2
            }
        );
        assert_eq!(
            top.networks[1].1,
            Totals {
                backend_seconds: 4.0,
                response_bytes: 10,
                requests: 1
            }
        );
        assert_eq!(
            top.other,
            Totals {
                backend_seconds: 1.5,
                response_bytes: 107,
                requests: 3
            }
        );
    }

    #[test]
    fn the_top_follows_recent_spending_and_grand_totals_are_kept() {
        let tracker = ClientCostTracker::new(1, Duration::from_secs(60));
        let start = Instant::now();
        tracker.record(net("203.0.113.1"), 10.0, 100, start);
        assert_eq!(labels(&tracker, start), ["203.0.113.0/24"]);

        // an hour later the old spender has decayed far below a new, smaller one
        let later = start + Duration::from_secs(3600);
        tracker.record(net("198.51.100.1"), 1.0, 10, later);
        let top = tracker.top(later);
        assert_eq!(labels(&tracker, later), ["198.51.100.0/24"]);
        assert_eq!(
            top.other,
            Totals {
                backend_seconds: 10.0,
                response_bytes: 100,
                requests: 1
            }
        );

        // the forgotten network's totals stay in other, and it restarts from zero when it returns
        tracker.record(net("203.0.113.1"), 0.5, 1, later);
        let top = tracker.top(later);
        assert_eq!(
            top.other,
            Totals {
                backend_seconds: 10.5,
                response_bytes: 101,
                requests: 2
            }
        );
    }

    #[test]
    fn many_networks_export_at_most_n_labels() {
        let tracker = ClientCostTracker::new(20, Duration::from_secs(300));
        let now = Instant::now();
        for i in 0..200u32 {
            tracker.record(
                net(&format!("10.{}.{}.1", i / 256, i % 256)),
                f64::from(i + 1),
                1,
                now,
            );
        }
        let top = tracker.top(now);
        assert_eq!(top.networks.len(), 20);
        assert_eq!(top.networks[0].0.to_string(), "10.0.199.0/24");
        assert_eq!(top.networks[19].0.to_string(), "10.0.180.0/24");
        let exported_requests: u64 = top
            .networks
            .iter()
            .map(|(_, totals)| totals.requests)
            .sum::<u64>()
            + top.other.requests;
        assert_eq!(exported_requests, 200);
    }

    #[test]
    fn config_switch_and_validation() {
        let config = |yaml: &str| {
            serde_yaml::from_str::<crate::config::DroneConfig>(&format!(
            "{{port: 0, hostname: h, cache_max_capacity: 1, operator_message: m, middleware_connection_threads: 1, {yaml}}}")).unwrap()
        };
        assert!(
            ClientCostTracker::from_config(&config("metrics_enabled: true"))
                .unwrap()
                .is_some(),
            "on by default"
        );
        assert!(ClientCostTracker::from_config(&config(
            "metrics_enabled: true, client_cost_metrics: false"
        ))
        .unwrap()
        .is_none());
        assert!(
            ClientCostTracker::from_config(&config("metrics_enabled: false"))
                .unwrap()
                .is_none()
        );
        assert!(ClientCostTracker::from_config(&config(
            "metrics_enabled: true, client_cost_window_seconds: 0"
        ))
        .is_err());
    }
}
