# docker build -f docker/full.Dockerfile -t iceberg-catalog-local:latest .
FROM rust:1.79-slim-bookworm AS chef
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN apt update -q && \
    DEBIAN_FRONTEND=noninteractive apt install -yqq curl libpq-dev pkg-config libssl-dev make perl --no-install-recommends && \
    cargo install --version=0.7.4 sqlx-cli --no-default-features --features postgres
RUN cargo install cargo-chef 

WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .

ENV SQLX_OFFLINE=true
RUN cargo build --release --bin iceberg-catalog

# our final base
FROM gcr.io/distroless/cc-debian12:nonroot


# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
