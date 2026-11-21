#!/usr/bin/env bash
# Run drone's Rust tests. Bound to the quick, full, canary, baseline and
# coverage slots through the `command` adapter (see .aidev/project.yaml). AIDEV
# runs this from the repository root inside the image .aidev/runtime/Dockerfile
# builds, and reads the exit code as the verdict.
#
#   .aidev/run-tests.sh            # the tests
#   .aidev/run-tests.sh --release  # also `cargo check --release`: the release
#                                  # build is what the Dockerfile/CI ship
#
# Runs outside the image too, given cargo and cargo-nextest.
set -euo pipefail

cd "$(dirname "$0")/.."

release_check=false
if [ "${1:-}" = "--release" ]; then
    release_check=true
fi

# Last-run-wins: a report left by an earlier run must not be read as this one's.
rm -rf test-results

# `< /dev/null`: the suite otherwise inherits the pipe AIDEV feeds its script
# through (aidev#13728). The junit report lands in
# test-results/nextest/aidev/junit.xml (.config/nextest.toml).
cargo nextest run --locked --profile aidev < /dev/null

if $release_check; then
    cargo check --locked --release < /dev/null
fi
