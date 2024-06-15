FROM rust:1.78 AS chef

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
