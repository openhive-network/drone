FROM rust:bullseye as builder

WORKDIR /drone

COPY . /drone

RUN cargo build --release

FROM debian:bullseye-slim AS runner

COPY --from=builder /drone/target/release/drone /drone

EXPOSE 3000

CMD ["./drone"]

ARG BUILD_TIME
ARG GIT_COMMIT_SHA
ARG GIT_CURRENT_BRANCH
ARG GIT_LAST_LOG_MESSAGE
ARG GIT_LAST_COMMITTER
ARG GIT_LAST_COMMIT_DATE
LABEL org.opencontainers.image.created="$BUILD_TIME"
LABEL org.opencontainers.image.url="https://hive.io/"
LABEL org.opencontainers.image.documentation="https://gitlab.syncad.com/hive/drone"
LABEL org.opencontainers.image.source="https://gitlab.syncad.com/hive/drone"
#LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="$GIT_COMMIT_SHA"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.ref.name="Drone API cache for Hive"
LABEL org.opencontainers.image.title="Drone JSON-RPC API caching reverse-proxy for Hive and HAF"
LABEL org.opencontainers.image.description="Blockchain-aware caching reverse proxy for the HIVE/HAF JSON-RPC calls, fills a similar role to jussi"
LABEL io.hive.image.branch="$GIT_CURRENT_BRANCH"
LABEL io.hive.image.commit.log_message="$GIT_LAST_LOG_MESSAGE"
LABEL io.hive.image.commit.author="$GIT_LAST_COMMITTER"
LABEL io.hive.image.commit.date="$GIT_LAST_COMMIT_DATE"
