#!/usr/bin/env bash
# Run drone's Rust tests. Bound to the quick, full, canary, baseline and
# coverage slots through the `command` adapter (see .aidev/project.yaml). AIDEV
# runs this from the repository root inside the image .aidev/runtime/Dockerfile
# builds, and reads the exit code as the verdict.
#
#   .aidev/run-tests.sh            # the tests
#   .aidev/run-tests.sh --release  # also `cargo check --release`: the release
#                                  # build is what the Dockerfile/CI ship
#   .aidev/run-tests.sh --coverage # the tests under cargo-llvm-cov, writing a
#                                  # Cobertura coverage.xml to the artefacts dir
#
# Any further arguments go to `cargo nextest run` (e.g. a test name filter).
#
# Runs outside the image too, given cargo and cargo-nextest (and cargo-llvm-cov
# for --coverage).
set -euo pipefail

cd "$(dirname "$0")/.."

mode=""
case "${1:-}" in
    --release | --coverage) mode="$1"; shift ;;
esac

# Last-run-wins: a report left by an earlier run must not be read as this one's.
rm -rf test-results

# `< /dev/null`: the suite otherwise inherits the pipe AIDEV feeds its script
# through (aidev#13728). The junit report lands in
# test-results/nextest/aidev/junit.xml (.config/nextest.toml).
case "$mode" in
    "")
        cargo nextest run --locked --profile aidev "$@" < /dev/null
        ;;
    --release)
        cargo nextest run --locked --profile aidev "$@" < /dev/null
        cargo check --locked --release < /dev/null
        ;;
    --coverage)
        # AIDEV reads the report from coverage.xml in AIDEV_SUITE_ARTIFACTS
        # (.aidev-artifacts/ in the workspace when it is not set).
        artifacts="${AIDEV_SUITE_ARTIFACTS:-.aidev-artifacts}"
        mkdir -p "$artifacts"
        rm -f "$artifacts/coverage.xml"
        cargo llvm-cov nextest --locked --profile aidev \
            --cobertura --output-path "$artifacts/coverage.xml" "$@" < /dev/null
        ;;
esac
