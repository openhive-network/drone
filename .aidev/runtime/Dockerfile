# The image AIDEV runs drone's test slots in (.aidev/project.yaml,
# `environment: kind: container`). AIDEV runs it with no network, as an
# arbitrary uid, with the checkout mounted at /work, so everything cargo needs
# is in the image: the toolchain, cargo-nextest (for junit), the crates.io
# dependencies of Cargo.lock, and those dependencies already compiled. A run
# then compiles only drone itself.
#
# Rebuild and re-pin it whenever Cargo.lock changes: .aidev/runtime/build.sh.
FROM rust@sha256:4730e387a220a08a365c77da3096544dde214f9d796c16284d4be45438cad4a9
# ^ rust:bullseye, the toolchain the project's own Dockerfile builds with.

ARG NEXTEST_VERSION=0.9.148
RUN curl -fsSL "https://get.nexte.st/${NEXTEST_VERSION}/linux-musl" | tar -xz -C "${CARGO_HOME}/bin" \
 && cargo nextest --version

ENV CARGO_TARGET_DIR=/opt/target
WORKDIR /work
COPY Cargo.toml Cargo.lock ./
# Compile the dependencies for the profiles the slots use (test, and the release
# `cargo check`), against a stub crate, then drop the stub's own output.
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && cargo fetch --locked \
 && cargo build --locked --tests \
 && cargo check --locked --release \
 && rm -rf src Cargo.toml Cargo.lock \
 && find "$CARGO_TARGET_DIR" -name 'drone*' -prune -exec rm -rf {} + \
 && chmod -R a+rwX "$CARGO_TARGET_DIR" "$CARGO_HOME"

# No network at test time, any uid, no passwd entry.
ENV CARGO_NET_OFFLINE=true HOME=/tmp
