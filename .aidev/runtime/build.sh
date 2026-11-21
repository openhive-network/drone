#!/usr/bin/env bash
# Build and push the image AIDEV runs drone's tests in, then print the
# digest to pin as `environment.image` in .aidev/project.yaml. Run it from any
# directory after a change to Cargo.lock or .aidev/runtime/Dockerfile:
#
#   docker login registry.gitlab.syncad.com
#   .aidev/runtime/build.sh
set -euo pipefail

cd "$(dirname "$0")/../.."
image="${IMAGE:-registry.gitlab.syncad.com/hive/drone/aidev-tests}"
tag="$(sha256sum Cargo.lock .aidev/runtime/Dockerfile | sha256sum | cut -c1-12)"

# Build through the region's GitLab image cache (:5001): FROM registry.gitlab.syncad.com/... stays
# canonical and BuildKit fetches its blobs from the cache. AIDEV_IMAGE_CACHE_BY_REGION
# (region=host:port,...; a region matches a label of the host's FQDN, * any other host)
# names the caches; unset, the build goes straight to the registry.
region_build() {
  local cache_map="${AIDEV_IMAGE_CACHE_BY_REGION:-}"
  local region="*" label entry cache="" builder config
  local -a entries
  if [ -z "$cache_map" ]; then
    docker build "$@"
    return
  fi
  IFS=, read -r -a entries <<<"$cache_map"
  for label in $( (hostname -f 2>/dev/null || hostname) | tr 'A-Z.' 'a-z '); do
    for entry in "${entries[@]}"; do
      if [ "${entry%%=*}" = "$label" ]; then region="$label"; fi
    done
  done
  for entry in "${entries[@]}"; do
    if [ "${entry%%=*}" = "$region" ]; then cache="${entry#*=}"; fi
  done
  if [ -z "$cache" ]; then
    echo "region_build: no image cache serves region $region; building straight from registry.gitlab.syncad.com" >&2
    docker build "$@"
    return
  fi
  builder="aidev-region-build-${cache//[.:]/-}"
  if ! docker buildx inspect "$builder" >/dev/null 2>&1; then
    config="$(mktemp)"
    printf '[registry."%s"]\n  mirrors = ["%s"]\n\n[registry."%s"]\n  http = true\n  insecure = true\n' \
      "registry.gitlab.syncad.com" "$cache" "$cache" >"$config"
    docker buildx create --name "$builder" --driver docker-container --config "$config" >/dev/null
    rm -f "$config"
  fi
  docker buildx build --builder "$builder" --load "$@"
}

# Push <image>:<tag>, print <image>@sha256:<digest>, and seed the build region's cache
# with that digest (aidev image seed-region-cache) so the region's first pull is a LAN hit.
region_push() {
  local ref
  docker push -q "$1" >/dev/null
  ref="$(docker image inspect "$1" | grep -o "\"${1%:*}@sha256:[0-9a-f]*\"" | head -1 | tr -d '"' || true)"
  if [ -z "$ref" ]; then
    echo "region_push: docker recorded no digest for $1 after the push" >&2
    return 1
  fi
  if [ -n "${AIDEV_IMAGE_CACHE_BY_REGION:-}" ] && [ -z "${AIDEV_RUNTIME_SEED_BY_CALLER:-}" ] && command -v aidev >/dev/null 2>&1; then
    aidev image seed-region-cache "$ref" >&2 || echo "region_push: could not seed the region cache with $ref; the region fetches it from the registry" >&2
  fi
  echo "$ref"
}

region_build -t "$image:$tag" -f .aidev/runtime/Dockerfile .
region_push "$image:$tag"
