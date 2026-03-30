#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Configure jemalloc: enable background thread for page purging, reduce dirty/muzzy
// page decay from 10s to 1s so freed memory is returned to the OS faster.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000,prof:true,prof_active:false,lg_prof_sample:21\0";

use actix_cors::Cors;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer, Responder, http::StatusCode, middleware::Condition, dev::Extensions};
use quick_cache::sync::Cache as QuickCache;
use ureq::Agent;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json, value::RawValue};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
//use actix_web::rt::time::sleep;
use tokio::sync::RwLock;
use chrono::DateTime;
use actix_request_identifier::{RequestId, RequestIdentifier, IdReuse};
use sha256::digest;

use log::{error, warn, info, debug, trace, log_enabled, Level::Info};



pub mod config;
use config::{AppConfig, TtlValue};

pub mod method_renamer;
use method_renamer::MethodAndParams;

pub mod metrics;
use metrics::DroneMetrics;

const DRONE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Stored as connection-level extension data via HttpServer::on_connect.
/// Used to track connection age and force recycling when max_conn_lifetime_secs is set.
#[derive(Clone)]
struct ConnBirthTime(Instant);


struct BlockchainState {
    last_irreversible_block_number: u32,
    head_block_number: u32,
    head_block_time: SystemTime
}

impl BlockchainState {
    pub fn new() -> BlockchainState {
        BlockchainState {
            last_irreversible_block_number: 0,
            head_block_number: 0,
            head_block_time: SystemTime::UNIX_EPOCH
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct HealthCheck {
    status: String,
    drone_version: String,
    message: String,
}

// Use Index for both / and /health.
async fn index(appdata: web::Data<AppData>) -> impl Responder {
    // Reply with health check JSON.
    HttpResponse::Ok().json(HealthCheck {
        status: "OK".to_string(),
        drone_version: DRONE_VERSION.to_string(),
        message: appdata.config.drone.operator_message.to_string(),
    })
}

async fn cache_size(appdata: web::Data<AppData>) -> impl Responder {
    HttpResponse::Ok().json(json!({"current_size": appdata.cache.weight(), "max_size": appdata.config.drone.cache_max_capacity}))
}

// return a list of cache keys and their sizes.  Can be huge
async fn cache_entries(appdata: web::Data<AppData>) -> impl Responder {
    let entries: Vec<Value> = appdata.cache.iter().map(|(key, value)| {
        json!({
            "key": (*key).to_string(),
            "size": value.size
        })
    }).collect();

    HttpResponse::Ok().json(entries)
}

async fn metrics_handler(appdata: web::Data<AppData>) -> impl Responder {
    let metrics_text = appdata.metrics.encode();
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(metrics_text)
}

/// Returns jemalloc internal memory counters.  Zero overhead — reads cached stats.
/// Key insight: if allocated grows linearly, it's a leak.  If allocated is stable
/// but resident grows, it's fragmentation.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
async fn jemalloc_stats_handler() -> impl Responder {
    tikv_jemalloc_ctl::epoch::advance().unwrap();

    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let mapped = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);
    let retained = tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0);

    HttpResponse::Ok().json(json!({
        "allocated_bytes": allocated,
        "active_bytes": active,
        "resident_bytes": resident,
        "mapped_bytes": mapped,
        "retained_bytes": retained,
        "allocated_mb": allocated / (1024 * 1024),
        "active_mb": active / (1024 * 1024),
        "resident_mb": resident / (1024 * 1024),
        "mapped_mb": mapped / (1024 * 1024),
        "retained_mb": retained / (1024 * 1024),
        "fragmentation_bytes": resident.saturating_sub(allocated),
        "fragmentation_pct": if allocated > 0 { ((resident.saturating_sub(allocated)) as f64 / allocated as f64) * 100.0 } else { 0.0 }
    }))
}

/// Triggers a jemalloc heap profile dump and returns the raw .heap file.
/// Profiling starts disabled; first call activates it.  ~4.5% overhead when active.
/// Analyze with: jeprof --text /path/to/drone dump.heap
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
async fn heap_profile_handler() -> impl Responder {
    use std::ffi::CString;

    // Activate profiling if not already active
    let currently_active: bool = unsafe { tikv_jemalloc_ctl::raw::read(b"prof.active\0") }.unwrap_or(false);
    if !currently_active {
        if let Err(e) = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", true) } {
            return HttpResponse::InternalServerError()
                .json(json!({"error": format!("Failed to activate profiling: {}", e)}));
        }
        info!("jemalloc heap profiling activated");
    }

    // Dump profile to temp file
    let path = format!("/tmp/drone_heap_{}.heap", std::process::id());
    let path_cstr = match CString::new(path.clone()) {
        Ok(c) => c,
        Err(e) => return HttpResponse::InternalServerError()
            .json(json!({"error": format!("Invalid path: {}", e)})),
    };
    if let Err(e) = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path_cstr.as_ptr()) } {
        return HttpResponse::InternalServerError()
            .json(json!({"error": format!("Failed to dump profile: {}", e)}));
    }

    // Read and return the file
    match std::fs::read(&path) {
        Ok(data) => {
            let _ = std::fs::remove_file(&path);
            HttpResponse::Ok()
                .content_type("application/octet-stream")
                .insert_header(("Content-Disposition", "attachment; filename=heap.prof"))
                .body(data)
        }
        Err(e) => HttpResponse::InternalServerError()
            .json(json!({"error": format!("Failed to read profile dump: {}", e)}))
    }
}

// Enum for API Requests, either single or batch.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum APICall {
    Single(APIRequest),
    Batch(Vec<APIRequest>),
}

// Enum for id in JSONRPC body.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum ID {
    Str(String),
    Int(u32),
}


// Structure for API calls.
#[derive(Serialize, Deserialize, Debug)]
pub struct APIRequest {
    jsonrpc: String,
    id: ID,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
enum ErrorField {
    Object(Value),   // JSON from Hived
    Message(String), // Custom message
}

impl Serialize for ErrorField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ErrorField::Object(json_value) => json_value.serialize(serializer),
            ErrorField::Message(text) => text.serialize(serializer),
        }
    }
}

// data returned just for logging/debugging
#[derive(Clone,Debug)]
struct ResponseTrackingInfo {
    cached: bool,
    mapped_method: MethodAndParams, // the method, parsed and transformed
    backend_url: Option<String>,
    upstream_method: Option<String>
}

impl ResponseTrackingInfo {
    fn into_headers(self, reply_builder: &mut HttpResponseBuilder) {
        reply_builder.insert_header(("X-Jussi-Cache-Hit", self.cached.to_string()));
        reply_builder.insert_header(("X-Jussi-Namespace", self.mapped_method.namespace));
        reply_builder.insert_header(("X-Jussi-Api", self.mapped_method.api.unwrap_or("<Empty>".to_string())));
        reply_builder.insert_header(("X-Jussi-Method", self.mapped_method.method));
        // removed params because it can be huge for posts, and can easily overflow nginx 
        // proxy buffer
        // reply_builder.insert_header(("X-Jussi-Params", self.mapped_method.params.map_or("[]".to_string(), |v| v.to_string())));
        // instead print the hash of the parameters, we can use that to tell which calls are
        // identical even if we don't know exactly what the parameters were
        reply_builder.insert_header(("X-Jussi-Param-Hash", digest(self.mapped_method.params.map_or("[]".to_string(), |v| v.to_string()))));
        if self.backend_url.is_some() {
            reply_builder.insert_header(("X-Jussi-Backend-Url", self.backend_url.unwrap()));
        }
        if self.upstream_method.is_some() {
            reply_builder.insert_header(("X-Jussi-Upstream-Method", self.upstream_method.unwrap()));
        }
    }
}

// ErrorData and ApiCallResponseData are the values stored in the cache.  It's
// everything about a reply that isn't specific to the caller (i.e., not the
// `jsonrpc` and `id` fields).
// NOTE: tracking_info is deliberately NOT stored here — it contains MethodAndParams
// with a serde_json::Value (request params) that causes unbounded memory growth when
// cloned inside moka's concurrent hash map (crossbeam-epoch GC can't reclaim fast enough).
// Instead, tracking_info is reconstructed from the mapped_method in handle_request().
#[derive(Clone, Debug)]
struct ErrorData {
    error: Value,
    http_status: StatusCode,
    /// Duration of the backend call in seconds (None if cached or no upstream call was made)
    backend_duration_secs: Option<f64>
}

#[derive(Clone, Debug)]
struct ApiCallResponseData {
    /// The "result" field from the upstream JSON-RPC response, stored as a raw JSON string.
    /// This avoids the 3-4x memory overhead of serde_json::Value and makes the cache
    /// weigher accurate (string bytes ≈ actual heap usage).
    result_json: String,
    /// Duration of the backend call in seconds (None if cached)
    backend_duration_secs: Option<f64>
}

// The full error and response structures, including caller-specific data
#[derive(Debug, Clone)]
struct ErrorStructure {
    jsonrpc: String,
    id: ID,
    error: Value,
    http_status: StatusCode,
    tracking_info: Option<ResponseTrackingInfo>
}

#[derive(Clone)]
struct APICallResponse {
    /// the original value of jsonrpc request made by the caller (usually "2.0")
    jsonrpc: String,
    /// the id the caller used in their request
    id: ID,

    /// The "result" field as a raw JSON string, ready to embed directly in the response
    result_json: String,

    tracking_info: Option<ResponseTrackingInfo>
}

/// Targeted deserialization of upstream JSON-RPC responses.  The "result" field
/// is kept as an unparsed raw JSON string (via RawValue), avoiding the 3-4x memory
/// overhead of building a full serde_json::Value tree for every response.
/// Only the ~1% of requests that need deep inspection (get_dynamic_global_properties,
/// EXPIRE_IF_REVERSIBLE) will subsequently parse the result.
#[derive(Deserialize)]
struct UpstreamJsonRpcResponse {
    result: Option<Box<RawValue>>,
    error: Option<Value>,
}

#[derive(Debug, Copy, Clone)]
enum CacheTtl {
    NoCache,
    NoExpire,
    CacheForDuration(Duration)
}

#[derive(Clone, Debug)]
struct CacheEntry {
    result: Result<ApiCallResponseData, ErrorData>,
    size: u32,
    ttl: CacheTtl,
    inserted_at: Instant,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        match self.ttl {
            CacheTtl::NoCache => true,
            CacheTtl::NoExpire => false,
            CacheTtl::CacheForDuration(duration) => self.inserted_at.elapsed() > duration,
        }
    }
}

#[derive(Clone)]
struct CacheWeighter;

impl quick_cache::Weighter<String, CacheEntry> for CacheWeighter {
    fn weight(&self, _key: &String, value: &CacheEntry) -> u64 {
        value.size as u64
    }
}

/// This is a helper function used for ExpireIfReversible methods.  This is called on the result
/// of the backend call to get the block number of the item returned
fn get_block_number_from_result(result: &Value, request_id: &RequestId) -> Option<u32> {
    // appbase get_block
    if let Some(block_num) = result.pointer("/block/block_id").and_then(|block_id| block_id.as_str()).and_then(|id_str| u32::from_str_radix(&id_str[..8], 16).ok()) {
        return Some(block_num);
    }
    // appbase get_block_header
    if let Some(prev_block_num) = result.pointer("/header/previous").and_then(|block_id| block_id.as_str()).and_then(|id_str| u32::from_str_radix(&id_str[..8], 16).ok()) {
        return Some(prev_block_num + 1);
    }
    // hived get_block
    if let Some(block_num) = result.pointer("/block_id").and_then(|block_id| block_id.as_str()).and_then(|id_str| u32::from_str_radix(&id_str[..8], 16).ok()) {
        return Some(block_num);
    }
    // hived get_block_header
    if let Some(prev_block_num) = result.pointer("/previous").and_then(|block_id| block_id.as_str()).and_then(|id_str| u32::from_str_radix(&id_str[..8], 16).ok()) {
        return Some(prev_block_num + 1);
    }

    error!(request_id=request_id.as_str(); "get_block_number_from_result() was unable to find the block number.  This may mean you marked an unsupported method as ExpireIfReversible");
    None
}

// check a request to see if it's asking for a block that doesn't exist yet.  We get a lot of API
// calls that do this, presumably clients that are just polling for the next block.
// This is a case we can optimize.  Either by:
// - returning a stock error reply without contacting the upstream, or
// - if the block is expected to arrive in a few seconds, just wait.  once the block arrives, return it
// Waiting seems better, because if we don't, the client will probably just make the same request
// again (maybe after a short sleep).  And if we do it right, it may give them the block sooner
// than their polling loop would have.
async fn check_for_future_block_requests(mapped_method: &MethodAndParams, data: &web::Data<AppData>, request_id: &RequestId) {
    if mapped_method.method == "get_block" {
        if let Some(block_num) = mapped_method.params.as_ref().and_then(|v| v["block_num"].as_u64()) {
            let current_head_block_number = data.blockchain_state.read().await.head_block_number;
            if block_num as u32 > current_head_block_number {
                // we're only testing against the head block number we recorded the last
                // time someone called get_dynamic_global_properties.
                // we should also check that now() is < the predicted time the requested
                // block will be produced
                info!(request_id=request_id.as_str(); "future block requested: {block_num}, head is {current_head_block_number}");
            }
        }
    }
}

async fn request_from_upstream(data: web::Data<AppData>, mapped_method: MethodAndParams, method_and_params_str: String, request_id: &RequestId) -> CacheEntry {
    let backend_start = Instant::now();

    let backend = match data.config.lookup_backend(mapped_method.get_method_name_parts()) {
        Some(backend) => { backend }
        None => {
            return CacheEntry {
                result: Err(ErrorData {
                    error: json!({
                        "code": -32603, // or 32601?
                        "message": "Unable to map request to endpoint.",
                        "error": "Unable to map request to endpoint."
                    }),
                    http_status: StatusCode::NOT_FOUND,
                    backend_duration_secs: None
                }),
                size: 0,
                ttl: CacheTtl::NoCache,
                inserted_at: Instant::now(),
            };
        }
    };

    let upstream_request = mapped_method.format_for_upstream(&data.config);
    debug!(request_id=request_id.as_str(); "Making upstream request for {method_and_params_str}");

    // Use ureq (blocking HTTP client) via spawn_blocking to avoid hyper's per-connection
    // tokio task leak.  ureq uses std::net::TcpStream directly — when the request completes,
    // all buffers are freed immediately with no async task lifecycle issues.
    let agent = data.webclient.clone();
    let backend_url = backend.url.clone();
    let request_json = serde_json::to_string(&upstream_request).unwrap();

    let body = match tokio::task::spawn_blocking(move || -> Result<String, String> {
        agent.post(&backend_url)
            .set("Content-Type", "application/json")
            .send_string(&request_json)
            .map_err(|e| e.to_string())?
            .into_string()
            .map_err(|e| e.to_string())
    }).await {
        Ok(Ok(text)) => text,
        Ok(Err(err)) => {
            let error_message = format!("{err} {}", backend.url);
            debug!(request_id=request_id.as_str(); "Error making upstream request: {error_message}");
            let backend_duration_secs = backend_start.elapsed().as_secs_f64();
            return CacheEntry {
                result: Err(ErrorData {
                    error: json!({
                        "code": -32700,
                        "message": "Unable to send request to endpoint.",
                        "error": error_message
                    }),
                    http_status: StatusCode::SERVICE_UNAVAILABLE,
                    backend_duration_secs: Some(backend_duration_secs)
                }),
                size: 0,
                ttl: CacheTtl::NoCache,
                inserted_at: Instant::now(),
            };
        }
        Err(join_err) => {
            error!(request_id=request_id.as_str(); "spawn_blocking panicked: {join_err}");
            let backend_duration_secs = backend_start.elapsed().as_secs_f64();
            return CacheEntry {
                result: Err(ErrorData {
                    error: json!({
                        "code": -32700,
                        "message": "Internal error during upstream request.",
                        "error": join_err.to_string()
                    }),
                    http_status: StatusCode::INTERNAL_SERVER_ERROR,
                    backend_duration_secs: Some(backend_duration_secs)
                }),
                size: 0,
                ttl: CacheTtl::NoCache,
                inserted_at: Instant::now(),
            };
        }
    };
    // Deserialize using RawValue for the "result" field — this avoids building a full
    // serde_json::Value tree (3-4x memory overhead) for every upstream response.
    // Only the ~1% of requests needing deep inspection will parse the result later.
    let upstream_resp: UpstreamJsonRpcResponse = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            debug!(request_id=request_id.as_str(); "Unable to parse endpoint data: {err}");
            let backend_duration_secs = backend_start.elapsed().as_secs_f64();
            return CacheEntry {
                result: Err(ErrorData {
                    error: json!({
                        "code": -32602,
                        "message": "Unable to parse endpoint data.",
                        "error": err.to_string(),
                    }),
                    http_status: StatusCode::INTERNAL_SERVER_ERROR,
                    backend_duration_secs: Some(backend_duration_secs)
                }),
                size: 0,
                ttl: CacheTtl::NoCache,
                inserted_at: Instant::now(),
            };
        }
    };

    // Check for error response
    if let Some(ref error) = upstream_resp.error {
        if error.is_object() {
            trace!(request_id=request_id.as_str(); "Upstream response was an error: {}", error);
            let backend_duration_secs = backend_start.elapsed().as_secs_f64();
            return CacheEntry {
                result: Err(ErrorData {
                    error: upstream_resp.error.unwrap(),
                    http_status: StatusCode::OK,
                    backend_duration_secs: Some(backend_duration_secs)
                }),
                size: 0,
                ttl: CacheTtl::NoCache,
                inserted_at: Instant::now(),
            };
        }
    }

    // Get the raw result JSON string (zero-copy borrow from `body` via RawValue)
    let result_json_str = match upstream_resp.result {
        Some(ref raw) => raw.get(),
        None => "null",
    };

    let method_name_only = &mapped_method.method;
    debug!(request_id=request_id.as_str(); "Mapped method is {}", method_name_only);

    // Check for empty/null results that shouldn't be cached (using string comparison
    // instead of parsing into a Value)
    let is_empty_result = result_json_str == "null"
        || result_json_str == "[]"
        || result_json_str.starts_with("{\"blocks\":[]");

    // Look up TTL config first, so we know whether we need to parse the result
    let ttl_from_config = *data.config.lookup_ttl(mapped_method.get_method_name_parts()).unwrap_or(&TtlValue::NoCache);
    debug!(request_id=request_id.as_str(); "lookup_ttl for {method_and_params_str} returns {ttl_from_config:?}");

    // Only parse the result into a Value for the few methods that need deep inspection:
    // - get_dynamic_global_properties: extract blockchain state (LIB, head block, time)
    // - EXPIRE_IF_REVERSIBLE methods: extract block number from result
    // This avoids building the full Value tree for ~99% of requests.
    let needs_parsed_result = method_name_only == "get_dynamic_global_properties"
        || ttl_from_config == TtlValue::ExpireIfReversible;

    let parsed_result: Option<Value> = if needs_parsed_result {
        serde_json::from_str(result_json_str).ok()
    } else {
        None
    };

    // Update blockchain state from get_dynamic_global_properties
    if method_name_only == "get_dynamic_global_properties" {
        if let Some(ref result_value) = parsed_result {
            let new_lib = result_value["last_irreversible_block_num"].as_u64().map(|v| v as u32);
            let new_head = result_value["head_block_number"].as_u64().map(|v| v as u32);
            let new_time = result_value["time"].as_str();
            match (new_lib, new_head, new_time) {
                (Some(new_lib), Some(new_head), Some(new_time)) => {
                    let read_lock = data.blockchain_state.read().await;
                    if new_lib > read_lock.last_irreversible_block_number || new_head > read_lock.last_irreversible_block_number {
                        drop(read_lock);
                        let mut write_lock = data.blockchain_state.write().await;
                        write_lock.last_irreversible_block_number = new_lib;
                        if new_head != write_lock.head_block_number {
                            write_lock.head_block_number = new_head;
                            let current_head_block_time = DateTime::parse_from_rfc3339(&format!("{new_time}Z")).unwrap();
                            write_lock.head_block_time = SystemTime::from(current_head_block_time);
                        }
                        // Update blockchain state metrics
                        data.metrics.update_blockchain_state(new_head, new_lib);
                    }
                }
                _ => {
                    warn!(request_id=request_id.as_str(); "Invalid get_dynamic_global_properties result, ignoring");
                }
            }
        }
    }

    // Determine TTL
    let ttl = if is_empty_result {
        CacheTtl::NoCache
    } else {
        match ttl_from_config {
            TtlValue::NoCache => { CacheTtl::NoCache }
            TtlValue::NoExpire => { CacheTtl::NoExpire }
            TtlValue::ExpireIfReversible => {
                // we cache forever if the block is irreversible, or 9 seconds if it's reversible
                let block_number = parsed_result.as_ref().and_then(|v| get_block_number_from_result(v, request_id));
                if let Some(block_number) = block_number {
                    let last_irreversible_block_number = data.blockchain_state.read().await.last_irreversible_block_number;
                    if block_number > last_irreversible_block_number { CacheTtl::CacheForDuration(Duration::from_secs(9)) } else { CacheTtl::NoExpire }
                } else {
                    CacheTtl::NoCache
                }
            }
            TtlValue::HonorUpstreamCacheControl => { CacheTtl::NoCache /* TODO: implement this */ }
            TtlValue::DurationInSeconds(seconds) => { CacheTtl::CacheForDuration(Duration::from_secs(seconds as u64)) }
        }
    };

    // Own the result string before dropping the upstream response.
    // result_json_str borrows from upstream_resp via RawValue, so we must
    // copy it before we can free the response body.
    let result_json = result_json_str.to_owned();
    let result_json_len = result_json.len() as u32;

    // Drop temporaries: parsed Value (if any), upstream response (contains RawValue
    // borrowing from body), and then the body itself.
    drop(parsed_result);
    drop(upstream_resp);
    drop(body);

    // Record backend duration metrics
    let backend_duration = backend_start.elapsed().as_secs_f64();
    data.metrics.record_backend_duration(
        &mapped_method.namespace,
        mapped_method.api.as_deref().unwrap_or(""),
        &mapped_method.method,
        &backend.name,
        backend_duration
    );

    let backend_duration_secs = backend_duration;
    trace!(request_id=request_id.as_str(); "Upstream call succeeded, returning cache entry with ttl {:?}", ttl);
    CacheEntry {
        result: Ok(ApiCallResponseData {
            result_json,
            backend_duration_secs: Some(backend_duration_secs)
        }),
        size: result_json_len,
        ttl,
        inserted_at: Instant::now(),
    }
}

/// Build a JSON access log entry for file logging
fn build_access_log_json(
    client_ip: &str,
    mapped_method: &MethodAndParams,
    duration_seconds: f64,
    backend_time: Option<f64>,
    cached: bool,
    request_body_json: &str,
    request_id: &RequestId,
    status: u16,
) -> Value {
    let now = chrono::Local::now();
    let time_local = now.format("%d/%b/%Y:%H:%M:%S %z").to_string();

    json!({
        "time_local": time_local,
        "status": status,
        "request": "POST / HTTP/1.1",
        "remote_addr": client_ip,
        "namespace": mapped_method.namespace,
        "api": mapped_method.api.as_deref().unwrap_or(""),
        "method": mapped_method.method,
        "request_time": duration_seconds,
        "backend_time": backend_time,
        "cache_hit": cached,
        "request_body": request_body_json,
        "request_id": request_id.as_str()
    })
}

async fn handle_request(request: APIRequest, data: &web::Data<AppData>, client_ip: &String, request_id: &RequestId) -> Result<APICallResponse, ErrorStructure> {
    let request_start = Instant::now();
    data.metrics.inc_active_requests();

    // perform any requested mappings, this may give us different method names & and params
    let mapped_method = method_renamer::map_method_name(&data.config, &request.method, &request.params).map_err(|_| {
        data.metrics.dec_active_requests();
        ErrorStructure {
            jsonrpc: request.jsonrpc.clone(),
            id : request.id.clone(),
            error: json!({
                "code": -32700,
                "message": "Unable to parse request method.",
                "error": "Unable to parse request method."
            }),
            http_status: StatusCode::NOT_FOUND,
            tracking_info: None
        }
    })?;

    check_for_future_block_requests(&mapped_method, data, request_id).await;

    // Simple access log format (logs before request processing)
    if data.config.drone.access_log_format == "simple" && log_enabled!(target: "access_log", Info) {
        // Get humantime for logging.
        let human_timestamp = humantime::format_rfc3339_seconds(std::time::SystemTime::now());
        if let Some(params) = &request.params {
            info!(target: "access_log",
                  "Timestamp: {} || IP: {} || Request Method: {} || Request Params: {} || Request Id: {}",
                  human_timestamp, client_ip, request.method, params, request_id.as_str())
        } else {
            info!(target: "access_log",
                  "Timestamp: {} || IP: {} || Request Method: {} || Request Id: {}",
                  human_timestamp, client_ip, request.method, request_id.as_str())
        }
    }

    // Cache lookup with request deduplication via quick_cache's guard API.
    // get_value_or_guard_async returns either:
    //   Ok(entry) — cache hit (or another task already computing this key, shared result)
    //   Err(guard) — cache miss, we hold the guard, other callers for the same key wait on us
    //
    // TTL is checked manually: quick_cache doesn't have built-in per-entry TTL, so expired
    // entries are treated as misses and removed.  NoCache entries are never inserted.
    let params_str = request.params.as_ref().map_or("[]".to_string(), |v: &Value| v.to_string());
    let method_and_params_str = mapped_method.get_dotted_method_name() + "(" + &params_str + ")";

    let mut upstream_was_called = false;
    let cache_entry = match data.cache.get_value_or_guard_async(&method_and_params_str).await {
        Ok(entry) if !entry.is_expired() => {
            // Valid cache hit
            entry
        }
        Ok(_expired) => {
            // Expired entry — remove and fetch fresh
            data.cache.remove(&method_and_params_str);
            upstream_was_called = true;
            let entry = request_from_upstream(data.clone(), mapped_method.clone(), method_and_params_str.clone(), request_id).await;
            if !matches!(entry.ttl, CacheTtl::NoCache) {
                data.cache.insert(method_and_params_str.clone(), entry.clone());
            }
            entry
        }
        Err(guard) => {
            // Cache miss — we hold the guard, others wait on us
            upstream_was_called = true;
            let entry = request_from_upstream(data.clone(), mapped_method.clone(), method_and_params_str.clone(), request_id).await;
            if matches!(entry.ttl, CacheTtl::NoCache) {
                // Don't cache — drop guard so waiters retry with their own upstream call
                drop(guard);
            } else {
                let _ = guard.insert(entry.clone());
            }
            entry
        }
    };

    let duration_seconds = request_start.elapsed().as_secs_f64();
    data.metrics.dec_active_requests();

    // Serialize request body early for JSON access logging (before request is moved)
    // Needed for both console JSON logging and file logging
    let needs_json_logging = data.config.drone.access_log_format == "json" || data.access_log_writer.is_some();
    let request_body_json = if needs_json_logging {
        serde_json::to_string(&request).unwrap_or_default()
    } else {
        String::new()
    };

    match cache_entry.result {
        Ok(api_call_response) => {
            trace!(request_id=request_id.as_str(); "Result was a regular non-error response, upstream_was_called = {upstream_was_called}");
            let backend_duration_secs = api_call_response.backend_duration_secs;
            let cached = !upstream_was_called;

            // Record success metrics (before moving mapped_method into tracking_info)
            data.metrics.record_request_success(
                &mapped_method.namespace,
                mapped_method.api.as_deref().unwrap_or(""),
                &mapped_method.method,
                cached,
                duration_seconds
            );

            let backend_time: Option<f64> = if cached { None } else { backend_duration_secs };

            // Write to access log file if configured (always JSON format)
            if let Some(ref writer) = data.access_log_writer {
                let log_entry = build_access_log_json(
                    client_ip,
                    &mapped_method,
                    duration_seconds,
                    backend_time,
                    cached,
                    &request_body_json,
                    request_id,
                    200,
                );
                if let Ok(mut w) = writer.lock() {
                    let _ = writeln!(w, "{}", log_entry);
                    if data.access_log_flush_every_line {
                        let _ = w.flush();
                    }
                }
            }

            // JSON access log format for console (logs after request processing with full details)
            if data.config.drone.access_log_format == "json" && log_enabled!(target: "access_log", Info) {
                let log_entry = build_access_log_json(
                    client_ip,
                    &mapped_method,
                    duration_seconds,
                    backend_time,
                    cached,
                    &request_body_json,
                    request_id,
                    200,
                );
                info!(target: "access_log", "{}", log_entry);
            }

            // Construct tracking_info here (not in cache) to avoid cloning
            // MethodAndParams with its Value params inside moka's concurrent hash map
            let backend = data.config.lookup_backend(mapped_method.get_method_name_parts());
            let upstream_method = mapped_method.format_for_upstream(&data.config).method;
            let tracking_info = ResponseTrackingInfo {
                cached,
                mapped_method,
                backend_url: backend.map(|b| b.url.clone()),
                upstream_method: Some(upstream_method),
            };

            Ok(APICallResponse {
                jsonrpc: request.jsonrpc,
                id: request.id,
                result_json: api_call_response.result_json,
                tracking_info: Some(tracking_info),
            })
        }
        Err(error_data) => {
            trace!(request_id=request_id.as_str(); "Result was an error response, upstream_was_called = {upstream_was_called}, http status should be {}", error_data.http_status);
            let backend_duration_secs = error_data.backend_duration_secs;
            let error_http_status = error_data.http_status;
            let cached = !upstream_was_called;

            // Record error metrics - extract error code from JSON error object
            let error_code = error_data.error.get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-32000) as i32;
            data.metrics.record_request_error(
                &mapped_method.namespace,
                mapped_method.api.as_deref().unwrap_or(""),
                &mapped_method.method,
                error_code,
                cached,
                duration_seconds
            );

            let backend_time: Option<f64> = if cached { None } else { backend_duration_secs };
            let http_status = error_http_status.as_u16();

            // Write to access log file if configured (always JSON format)
            if let Some(ref writer) = data.access_log_writer {
                let log_entry = build_access_log_json(
                    client_ip,
                    &mapped_method,
                    duration_seconds,
                    backend_time,
                    cached,
                    &request_body_json,
                    request_id,
                    http_status,
                );
                if let Ok(mut w) = writer.lock() {
                    let _ = writeln!(w, "{}", log_entry);
                    if data.access_log_flush_every_line {
                        let _ = w.flush();
                    }
                }
            }

            // JSON access log format for console (logs after request processing with full details)
            if data.config.drone.access_log_format == "json" && log_enabled!(target: "access_log", Info) {
                let log_entry = build_access_log_json(
                    client_ip,
                    &mapped_method,
                    duration_seconds,
                    backend_time,
                    cached,
                    &request_body_json,
                    request_id,
                    http_status,
                );
                info!(target: "access_log", "{}", log_entry);
            }

            Err(ErrorStructure {
                jsonrpc: request.jsonrpc.clone(),
                id: request.id,
                error: error_data.error.clone(),
                http_status: error_http_status,
                tracking_info: None, // not worth reconstructing for error responses
            })
        }
    }
}

/// Check if the connection has exceeded its max lifetime and should be closed.
/// Returns true if we should add "Connection: close" to the response.
fn should_close_connection(req: &HttpRequest, max_lifetime: Duration) -> bool {
    if max_lifetime.is_zero() {
        return false;
    }
    match req.conn_data::<ConnBirthTime>() {
        Some(birth) => {
            let age = birth.0.elapsed();
            if age > max_lifetime {
                debug!("Connection age {:?} exceeds max lifetime {:?}, sending Connection: close", age, max_lifetime);
                true
            } else {
                false
            }
        }
        None => {
            warn!("conn_data::<ConnBirthTime>() returned None — on_connect may not be working");
            false
        }
    }
}

async fn api_call(
    req: HttpRequest,
    call: web::Json<APICall>,
    data: web::Data<AppData>,
    request_id: RequestId
) -> impl Responder {
    let get_cloudflare_ip = req.headers().get("CF-Connecting-IP");

    let client_ip = match get_cloudflare_ip {
        Some(ip) => ip.to_str().map(|ip| ip.to_string()),
        None => Ok(req.peer_addr().unwrap().ip().to_string()),
    };
    let user_ip = match client_ip {
        Ok(ip) => ip,
        Err(_) => {
            return HttpResponse::InternalServerError().json(json!({
                "jsonrpc": "2.0",
                "id": 0,
                "error": {
                    "code": -32000,
                    "message": "Internal Server Error",
                    "error": "Invalid Cloudflare Proxy Header."
                }
            }))
        }
    };

    // Force connection close if it has exceeded its max lifetime.  This recycles
    // actix-http's internal BytesMut buffers which grow monotonically on long-lived connections.
    let close_conn = should_close_connection(&req, Duration::from_secs(data.config.drone.max_conn_lifetime_secs));

    match call.0 {
        APICall::Single(request) => {
            let result = handle_request(request, &data, &user_ip, &request_id).await;
            match result {
                Ok(response) => {
                    let mut reply_builder = HttpResponse::Ok();
                    reply_builder.insert_header(("Drone-Version", DRONE_VERSION));
                    if close_conn {
                        reply_builder.force_close();
                    }
                    if data.config.drone.add_jussi_headers {
                        if let Some(tracking_info) = response.tracking_info {
                            tracking_info.into_headers(&mut reply_builder);
                        }
                    }
                    // Build the JSON-RPC response by embedding the raw result JSON string
                    // directly, avoiding a parse-then-reserialize round-trip
                    let id_json = serde_json::to_string(&response.id).unwrap();
                    reply_builder
                        .content_type("application/json")
                        .body(format!(r#"{{"jsonrpc":"{}","result":{},"id":{}}}"#,
                            response.jsonrpc, response.result_json, id_json))
                },
                Err(err) => {
                    debug!(request_id=request_id.as_str(); "Constructing HttpResponse for an Err, status should be {}", err.http_status);
                    let mut reply_builder = HttpResponse::build(err.http_status);
                    reply_builder.insert_header(("Drone-Version", DRONE_VERSION));
                    if close_conn {
                        reply_builder.force_close();
                    }
                    if let Some(tracking_info) = err.tracking_info {
                        tracking_info.into_headers(&mut reply_builder);
                    }
                    reply_builder.json(json!({
                        "jsonrpc": err.jsonrpc,
                        "id": err.id,
                        "error": err.error
                    }))
                }
            }
        }
        APICall::Batch(requests) => {
            if requests.len() > 100 {
                return HttpResponse::InternalServerError().json(json!({
                    "jsonrpc": "2.0".to_string(),
                    "id": 0,
                    "error": json!({
                        "code": -32600,
                        "message": "Request parameter error.",
                        "error": "Batch size too large, maximum allowed is 100."
                    }),
                }));
            }

            let mut responses: Vec<String> = Vec::new();
            // we'll say that the result was cached if all non-error responses came from the cache.
            // the "cached" property isn't particularly useful for batch requests, so don't
            // overthink it
            let mut cached = true;
            for request in requests {
                let result = handle_request(request, &data, &user_ip, &request_id).await;
                match result {
                    Ok(response) => {
                        if !response.tracking_info.map_or(false, |v| v.cached) {
                            cached = false;
                        }
                        let id_json = serde_json::to_string(&response.id).unwrap();
                        responses.push(format!(r#"{{"jsonrpc":"{}","result":{},"id":{}}}"#,
                            response.jsonrpc, response.result_json, id_json));
                    },
                    Err(err) => {
                        let id_json = serde_json::to_string(&err.id).unwrap();
                        let error_json = serde_json::to_string(&err.error).unwrap();
                        responses.push(format!(r#"{{"jsonrpc":"{}","id":{},"error":{}}}"#,
                            err.jsonrpc, id_json, error_json));
                    }
                }
            }
            let body = format!("[{}]", responses.join(","));
            let mut reply_builder = HttpResponse::Ok();
            reply_builder.insert_header(("Drone-Version", DRONE_VERSION));
            reply_builder.insert_header(("Cache-Status", cached.to_string()));
            if close_conn {
                reply_builder.force_close();
            }
            reply_builder
                .content_type("application/json")
                .body(body)
        }
    }
}

struct AppData {
    cache: QuickCache<String, CacheEntry, CacheWeighter>,
    webclient: Agent,
    config: AppConfig,
    blockchain_state: Arc<RwLock<BlockchainState>>,
    metrics: Arc<DroneMetrics>,
    access_log_writer: Option<Arc<Mutex<BufWriter<File>>>>,
    access_log_flush_every_line: bool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    // Load config.
    let app_config = config::parse_file("config.yaml");

    // Initialize metrics
    let metrics = Arc::new(DroneMetrics::new(&app_config.drone.metrics_namespace));

    // Create the cache using quick_cache (no crossbeam-epoch, no unbounded memory growth)
    let cache = QuickCache::with_weighter(
        100_000, // estimated items capacity
        app_config.drone.cache_max_capacity,
        CacheWeighter,
    );

    // Initialize access log file if configured
    // Use 32KB buffer (same as nginx default) for high-throughput scenarios
    const ACCESS_LOG_BUFFER_SIZE: usize = 32 * 1024;

    let access_log_writer = if !app_config.drone.access_log_file.is_empty() {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&app_config.drone.access_log_file)
            .unwrap_or_else(|e| panic!("Failed to open access log file '{}': {}", app_config.drone.access_log_file, e));
        info!("Access log file enabled: {}", app_config.drone.access_log_file);
        Some(Arc::new(Mutex::new(BufWriter::with_capacity(ACCESS_LOG_BUFFER_SIZE, file))))
    } else {
        None
    };

    // Spawn background task to flush access log every 5 seconds (unless flush_every_line is enabled)
    if let Some(ref writer) = access_log_writer {
        if !app_config.drone.access_log_flush_every_line {
            let writer_for_flush = Arc::clone(writer);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if let Ok(mut w) = writer_for_flush.lock() {
                        let _ = w.flush();
                    }
                }
            });
        }
    }

    let access_log_flush_every_line = app_config.drone.access_log_flush_every_line;

    let app_data = web::Data::new(AppData {
        cache,
        webclient: ureq::AgentBuilder::new()
            .max_idle_connections_per_host(app_config.drone.middleware_connection_threads)
            .timeout(Duration::from_secs(90))
            .build(),
        config: app_config.clone(),
        blockchain_state: Arc::new(RwLock::new(BlockchainState::new())),
        metrics,
        access_log_writer,
        access_log_flush_every_line,
    });

    // Spawn background task to update cache metrics every 15 seconds
    let app_data_for_metrics = app_data.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            let size_bytes = app_data_for_metrics.cache.weight();
            let entry_count = app_data_for_metrics.cache.len() as u64;
            app_data_for_metrics.metrics.update_cache_metrics(size_bytes, entry_count);
        }
    });

    let metrics_enabled = app_config.drone.metrics_enabled;
    let metrics_path = app_config.drone.metrics_path.clone();

    info!("Drone is running on port {}.", app_config.drone.port);
    if metrics_enabled {
        info!("Prometheus metrics enabled at {}", metrics_path);
    }

    HttpServer::new(move || {
        let mut app = App::new()
            .wrap(RequestIdentifier::with_uuid().use_incoming_id(IdReuse::UseIncoming))
            .wrap(Condition::new(app_config.drone.add_cors_headers, Cors::permissive()))
            .app_data(
                web::JsonConfig::default()
                    .content_type(|_| true)
                    .content_type_required(false)
                    .limit(1024 * 100),
            ) // 100kb
            .app_data(app_data.clone())
            .route("/", web::get().to(index))
            .route("/", web::post().to(api_call))
            .route("/health", web::get().to(index))
            .route("/cache-entries", web::get().to(cache_entries))
            .route("/cache-size", web::get().to(cache_size));

        // Add metrics endpoint if enabled
        if metrics_enabled {
            app = app.route(&metrics_path, web::get().to(metrics_handler));
        }

        // jemalloc debug endpoints (disabled by default, enable with debug_endpoints_enabled: true)
        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        if app_config.drone.debug_endpoints_enabled {
            app = app.route("/debug/jemalloc_stats", web::get().to(jemalloc_stats_handler));
            app = app.route("/debug/heap_profile", web::get().to(heap_profile_handler));
        }

        app
    })
    .on_connect(|_conn, ext: &mut Extensions| {
        ext.insert(ConnBirthTime(Instant::now()));
    })
    .bind((app_config.drone.hostname, app_config.drone.port))?
    .run()
    .await
}
