#!/usr/bin/env bash
# drone's static checks: formatting and clippy, warnings fatal. Bound to the
# static slot through the `command` adapter (see .aidev/project.yaml); AIDEV
# runs it from the repository root inside the image .aidev/runtime/Dockerfile
# builds, and reads the exit code as the verdict.
#
# Runs outside the image too, given cargo with the rustfmt and clippy components.
set -euo pipefail

cd "$(dirname "$0")/.."

# Both always run, so one report names every finding; either failing fails the slot.
status=0
cargo fmt --check < /dev/null || status=1
cargo clippy --locked --all-targets -- -D warnings < /dev/null || status=1
exit "$status"
