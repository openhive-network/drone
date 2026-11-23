# Automated contributions

This directory configures the automated development platform that contributes to this
repository. This note says which branches it uses here and how its work reaches the
project's main line.

## Branches

- `aidev/integration` — the integration branch (protected, cut from `develop`). Automated
  work lands here and nowhere else.
- `ai/*` — one branch per task, created and deleted by the platform. Do not commit to them.
- `session/*` — a session's accumulated work, landed on the integration branch as a batch.

## How work reaches the main line

Task and session branches never reach `aidev/integration` through merge requests; the
platform lands them after its own verification. `develop` is protected and fast-forward
only, so `aidev handoff drone` opens a merge request carrying a linear copy of
`aidev/integration` into `develop` (`branches.handoff`). When merging such a request by
hand, **keep the source branch** (it is protected). The release (`develop` → `master`,
version tags) stays this project's own process.

`.gitlab-ci.yml` starts no pipeline for `ai/*`, `session/*` and `_deploy-canary*`
branches; `aidev/integration` and everything else run as before.

## Tests and the test image

- `.aidev/run-tests.sh` runs `cargo nextest run --profile aidev` (junit to
  `test-results/`, `.config/nextest.toml`); `--release` adds `cargo check --release`,
  and `--coverage` runs the tests under `cargo llvm-cov`, writing a Cobertura
  `coverage.xml` to `.aidev-artifacts/`. It runs anywhere cargo and cargo-nextest
  (and cargo-llvm-cov for `--coverage`) are installed.
- `.aidev/run-static-checks.sh` runs `cargo fmt --check` and
  `cargo clippy --all-targets -- -D warnings`; either failing fails it.
- The pinned image does not carry rustfmt, clippy or cargo-llvm-cov yet, so
  `static` is unbound and `coverage` runs the plain tests until it is rebuilt
  with them.
- The platform runs the script inside
  `registry.gitlab.syncad.com/hive/drone/aidev-tests@sha256:…` with no network, so the
  image carries the toolchain and `Cargo.lock`'s dependencies. After changing
  `Cargo.lock` or `.aidev/runtime/Dockerfile`, run `.aidev/runtime/build.sh` (after
  `docker login registry.gitlab.syncad.com`) and put the digest it prints into
  `environment.image` in `project.yaml`.

## Suites run before work lands

Declared in `.aidev/project.yaml` under `verification`:

- `quick` — `command` .aidev/run-tests.sh
- `full` — `command` .aidev/run-tests.sh --release
- `baseline` — `command` .aidev/run-tests.sh
- `canary` — `command` .aidev/run-tests.sh --release
- `coverage` — `command` .aidev/run-tests.sh
