# CLAUDE.md - Drone Repository

## Project Overview

Drone is a JSON-RPC API caching reverse-proxy for the Hive blockchain, written in Rust. It replaces Jussi and acts as a middleware caching layer to reduce load on backend API nodes (hived, hivemind, HAF, etc.) by caching JSON-RPC responses with configurable TTL strategies.

**GitLab Project ID:** 446

## Tech Stack

- **Language:** Rust (Edition 2021)
- **Web Framework:** Actix Web 4.3.1
- **Cache:** Moka 0.12.5 (LRU with TTL)
- **Async Runtime:** Tokio 1.36.0
- **HTTP Client:** Reqwest 0.11.14
- **Serialization:** Serde JSON/YAML

## Directory Structure

```
drone/
├── src/
│   ├── main.rs              # HTTP handlers, caching logic, app entry point
│   ├── config.rs            # Configuration parsing and structures
│   └── method_renamer.rs    # JSON-RPC method parsing and routing
├── Cargo.toml               # Project manifest
├── Dockerfile               # Multi-stage Docker build
├── docker-compose.yml       # Docker Compose setup
├── config.example.yaml      # Example configuration
└── .gitlab-ci.yml           # GitLab CI/CD pipeline
```

## Development Commands

```bash
# Build
cargo build --release

# Run (requires config.yaml with backend endpoints)
cargo run --release

# Docker (recommended)
docker-compose up --build -d
```

## Configuration

Copy `config.example.yaml` to `config.yaml` and configure:
- **backends** - URLs for hived, hivemind, hafah, hafsql
- **urls** - Method-to-backend routing
- **ttls** - Cache TTL per method (NO_CACHE, NO_EXPIRE, EXPIRE_IF_REVERSIBLE, or seconds)
- **timeouts** - Backend response timeouts

## Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | Main application, HTTP handlers, caching logic |
| `src/config.rs` | YAML config parsing, AppConfig structure |
| `src/method_renamer.rs` | JSON-RPC method name parsing and routing |
| `config.example.yaml` | Reference configuration with all options |

## API Endpoints

- `GET /` or `GET /health` - Health check (status, version, operator message)
- `POST /` - Main JSON-RPC endpoint (single or batch requests)
- `GET /cache_size` - Current cache size and max capacity
- `GET /cache_entries` - List all cache entries with sizes

## Coding Conventions

- Async/await with Tokio runtime
- Shared state via `web::Data<AppData>`
- JSON-RPC error codes: -32600 to -32700
- Structured logging with request IDs
- Serde for JSON/YAML deserialization

## CI/CD Notes

**Pipeline Stages:**
1. `build` - Docker image with short commit SHA tag
2. `publish-develop` - Tag as `develop` on develop branch
3. `publish-release` - Push to GitLab and hive.blog registries on version tags

**Key Settings:**
- Uses `GIT_STRATEGY: fetch` with full clone depth
- Multi-stage Docker build (rust:bullseye builder, debian:bullseye-slim runner)
- Automatic pipeline on push (no need for `glab ci run`)

**Registry:** `$CI_REGISTRY/hive/drone`

## Cache Behavior

- LRU cache with configurable max capacity (default 4GB)
- TTL modes: NO_CACHE, NO_EXPIRE, EXPIRE_IF_REVERSIBLE, or fixed seconds
- EXPIRE_IF_REVERSIBLE: 9s if block is reversible, forever if irreversible
- Parameter-based cache keys using SHA256 hash

## Debug Headers (when `add_jussi_headers: true`)

- `X-Jussi-Cache-Hit` - Cache hit boolean
- `X-Jussi-Namespace`, `X-Jussi-Api`, `X-Jussi-Method` - Parsed method info
- `X-Jussi-Param-Hash` - SHA256 of parameters
