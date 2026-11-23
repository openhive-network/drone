use prometheus::{
    CounterVec, Gauge, GaugeVec, HistogramVec,
    Opts, Registry, HistogramOpts,
    TextEncoder, Encoder,
};

/// Default histogram buckets for request duration (in seconds)
/// Covers range from 1ms to 30s with good granularity
const REQUEST_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0
];

/// Histogram buckets for the size of streamed responses (in bytes), 64 KiB to 64 MiB
const STREAMED_RESPONSE_BYTES_BUCKETS: &[f64] = &[
    65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0
];

/// DroneMetrics holds all Prometheus metric collectors for Drone
pub struct DroneMetrics {
    pub registry: Registry,

    // Request counters
    pub requests_total: CounterVec,
    pub requests_cached_total: CounterVec,
    pub requests_errors_total: CounterVec,

    // Latency histograms
    pub request_duration_seconds: HistogramVec,
    pub backend_duration_seconds: HistogramVec,

    // Responses piped from the backend without buffering
    pub streamed_response_bytes: HistogramVec,

    // Cache gauges
    pub cache_size_bytes: Gauge,
    pub cache_entries: Gauge,

    // Active requests gauge
    pub active_requests: Gauge,

    // Blockchain state gauges
    pub blockchain_head_block: Gauge,
    pub blockchain_lib: Gauge,

    // Per-client-network cost limiter
    pub cost_limit_throttled_total: CounterVec,
    pub cost_limit_usage_ratio: GaugeVec,
}

impl DroneMetrics {
    pub fn new(namespace: &str) -> Self {
        let registry = Registry::new();

        // Request counters
        let requests_total = CounterVec::new(
            Opts::new("requests_total", "Total number of JSON-RPC requests")
                .namespace(namespace),
            &["namespace", "api", "method", "status"]
        ).expect("Failed to create requests_total counter");

        let requests_cached_total = CounterVec::new(
            Opts::new("requests_cached_total", "Total number of cache hits")
                .namespace(namespace),
            &["namespace", "api", "method"]
        ).expect("Failed to create requests_cached_total counter");

        let requests_errors_total = CounterVec::new(
            Opts::new("requests_errors_total", "Total number of error responses")
                .namespace(namespace),
            &["namespace", "api", "method", "error_code"]
        ).expect("Failed to create requests_errors_total counter");

        // Latency histograms
        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "request_duration_seconds",
                "End-to-end request latency in seconds"
            )
            .namespace(namespace)
            .buckets(REQUEST_DURATION_BUCKETS.to_vec()),
            &["namespace", "api", "method", "cached"]
        ).expect("Failed to create request_duration_seconds histogram");

        let backend_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "backend_duration_seconds",
                "Backend call latency in seconds"
            )
            .namespace(namespace)
            .buckets(REQUEST_DURATION_BUCKETS.to_vec()),
            &["namespace", "api", "method", "backend"]
        ).expect("Failed to create backend_duration_seconds histogram");

        let streamed_response_bytes = HistogramVec::new(
            HistogramOpts::new(
                "streamed_response_bytes",
                "Size in bytes of backend responses streamed to the client without being cached"
            )
            .namespace(namespace)
            .buckets(STREAMED_RESPONSE_BYTES_BUCKETS.to_vec()),
            &["namespace", "api", "method"]
        ).expect("Failed to create streamed_response_bytes histogram");

        // Cache gauges
        let cache_size_bytes = Gauge::new(
            format!("{}_cache_size_bytes", namespace),
            "Current cache memory usage in bytes"
        ).expect("Failed to create cache_size_bytes gauge");

        let cache_entries = Gauge::new(
            format!("{}_cache_entries", namespace),
            "Number of entries in cache"
        ).expect("Failed to create cache_entries gauge");

        // Active requests gauge
        let active_requests = Gauge::new(
            format!("{}_active_requests", namespace),
            "Number of in-flight requests"
        ).expect("Failed to create active_requests gauge");

        // Blockchain state gauges
        let blockchain_head_block = Gauge::new(
            format!("{}_blockchain_head_block", namespace),
            "Current head block number"
        ).expect("Failed to create blockchain_head_block gauge");

        let blockchain_lib = Gauge::new(
            format!("{}_blockchain_lib", namespace),
            "Last irreversible block number"
        ).expect("Failed to create blockchain_lib gauge");

        let cost_limit_throttled_total = CounterVec::new(
            Opts::new("cost_limit_throttled_total", "Cache-missing requests refused because the client network was over its cost budget")
                .namespace(namespace),
            &["network"]
        ).expect("Failed to create cost_limit_throttled_total counter");

        let cost_limit_usage_ratio = GaugeVec::new(
            Opts::new("cost_limit_usage_ratio", "Cost a client network has spent, as a fraction of its budget")
                .namespace(namespace),
            &["network", "resource"]
        ).expect("Failed to create cost_limit_usage_ratio gauge");

        // Register all metrics
        registry.register(Box::new(requests_total.clone())).expect("Failed to register requests_total");
        registry.register(Box::new(requests_cached_total.clone())).expect("Failed to register requests_cached_total");
        registry.register(Box::new(requests_errors_total.clone())).expect("Failed to register requests_errors_total");
        registry.register(Box::new(request_duration_seconds.clone())).expect("Failed to register request_duration_seconds");
        registry.register(Box::new(backend_duration_seconds.clone())).expect("Failed to register backend_duration_seconds");
        registry.register(Box::new(streamed_response_bytes.clone())).expect("Failed to register streamed_response_bytes");
        registry.register(Box::new(cache_size_bytes.clone())).expect("Failed to register cache_size_bytes");
        registry.register(Box::new(cache_entries.clone())).expect("Failed to register cache_entries");
        registry.register(Box::new(active_requests.clone())).expect("Failed to register active_requests");
        registry.register(Box::new(blockchain_head_block.clone())).expect("Failed to register blockchain_head_block");
        registry.register(Box::new(blockchain_lib.clone())).expect("Failed to register blockchain_lib");
        registry.register(Box::new(cost_limit_throttled_total.clone())).expect("Failed to register cost_limit_throttled_total");
        registry.register(Box::new(cost_limit_usage_ratio.clone())).expect("Failed to register cost_limit_usage_ratio");

        DroneMetrics {
            registry,
            requests_total,
            requests_cached_total,
            requests_errors_total,
            request_duration_seconds,
            backend_duration_seconds,
            streamed_response_bytes,
            cache_size_bytes,
            cache_entries,
            active_requests,
            blockchain_head_block,
            blockchain_lib,
            cost_limit_throttled_total,
            cost_limit_usage_ratio,
        }
    }

    /// Encode all metrics as Prometheus text format
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).expect("Failed to encode metrics");
        String::from_utf8(buffer).expect("Failed to convert metrics to string")
    }

    /// Record a successful request
    pub fn record_request_success(
        &self,
        namespace: &str,
        api: &str,
        method: &str,
        cached: bool,
        duration_seconds: f64
    ) {
        let cached_str = if cached { "true" } else { "false" };

        self.requests_total
            .with_label_values(&[namespace, api, method, "success"])
            .inc();

        if cached {
            self.requests_cached_total
                .with_label_values(&[namespace, api, method])
                .inc();
        }

        self.request_duration_seconds
            .with_label_values(&[namespace, api, method, cached_str])
            .observe(duration_seconds);
    }

    /// Record an error request
    pub fn record_request_error(
        &self,
        namespace: &str,
        api: &str,
        method: &str,
        error_code: i32,
        cached: bool,
        duration_seconds: f64
    ) {
        let cached_str = if cached { "true" } else { "false" };
        let error_code_str = error_code.to_string();

        self.requests_total
            .with_label_values(&[namespace, api, method, "error"])
            .inc();

        self.requests_errors_total
            .with_label_values(&[namespace, api, method, &error_code_str])
            .inc();

        self.request_duration_seconds
            .with_label_values(&[namespace, api, method, cached_str])
            .observe(duration_seconds);
    }

    /// Record backend call duration
    pub fn record_backend_duration(
        &self,
        namespace: &str,
        api: &str,
        method: &str,
        backend: &str,
        duration_seconds: f64
    ) {
        self.backend_duration_seconds
            .with_label_values(&[namespace, api, method, backend])
            .observe(duration_seconds);
    }

    /// Record the size of a response streamed from the backend
    pub fn record_streamed_response(
        &self,
        namespace: &str,
        api: &str,
        method: &str,
        bytes: u64
    ) {
        self.streamed_response_bytes
            .with_label_values(&[namespace, api, method])
            .observe(bytes as f64);
    }

    /// Update cache metrics
    pub fn update_cache_metrics(&self, size_bytes: u64, entry_count: u64) {
        self.cache_size_bytes.set(size_bytes as f64);
        self.cache_entries.set(entry_count as f64);
    }

    /// Update blockchain state metrics
    pub fn update_blockchain_state(&self, head_block: u32, lib: u32) {
        self.blockchain_head_block.set(head_block as f64);
        self.blockchain_lib.set(lib as f64);
    }

    /// Record a request refused by the cost limiter
    pub fn record_cost_limit_throttled(&self, network: &str) {
        self.cost_limit_throttled_total
            .with_label_values(&[network])
            .inc();
    }

    /// Replace the per-network budget usage with `ratios` of (network, resource, ratio)
    pub fn update_cost_limit_usage<'a>(&self, ratios: impl IntoIterator<Item = (&'a str, &'a str, f64)>) {
        self.cost_limit_usage_ratio.reset();
        for (network, resource, ratio) in ratios {
            self.cost_limit_usage_ratio
                .with_label_values(&[network, resource])
                .set(ratio);
        }
    }

    /// Increment active requests counter
    pub fn inc_active_requests(&self) {
        self.active_requests.inc();
    }

    /// Decrement active requests counter
    pub fn dec_active_requests(&self) {
        self.active_requests.dec();
    }
}
