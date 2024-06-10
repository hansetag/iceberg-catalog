# docker build -f docker/full.Dockerfile -t iceberg-rest-local:latest .
FROM rust:1.78 AS chef
# We only pay the installation cost once, 
# it will be cached from the second build onwards
RUN apt update -q && \
    apt install -yqq libpq-dev  && \
    cargo install --version=0.7.4 sqlx-cli --no-default-features --features postgres
RUN cargo install cargo-chef 

WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
ARG DATABASE_URL=postgres://postgres:postgres@host.docker.internal/postgres
ENV DATABASE_URL=$DATABASE_URL
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cd crates/iceberg-catalog && \
    echo "DATABASE_URL=$DATABASE_URL" && \
    sqlx database create && \
    sqlx migrate run && \
    cd -
RUN cargo build --release --bin iceberg-catalog

# our final base
FROM debian:bookworm-slim

# non-root user
ARG USERNAME=iceberg
ARG USER_UID=5001
ARG USER_GID=$USER_UID

# Install OS deps
RUN apt-get update -q && \
    apt-get install -y -q openssl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME

WORKDIR /home/$USERNAME

# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog .

# # set the startup command to run your binary
CMD ["./iceberg-catalog"]
