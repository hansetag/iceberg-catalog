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
FROM gcr.io/distroless/cc-debian12:nonroot as base

FROM scratch

ARG EXPIRES=Never
LABEL maintainer="moderation@hansetag.com" quay.expires-after=${EXPIRES}
# copy almost everything from the distroless container leaving behind libgomp libssl which we don't need and create CVE
# noise we cannot use rm since we don't have a shell, we cannot use the experimental --exclude syntax since podman
# doesn't seem to support it. So we end up with that thing below here.

COPY --from=base /home /etc /lib /sys /proc /boot /bin /dev /root /tmp /
COPY --from=base /usr/share /usr/
COPY --from=base /usr/lib/os-release /usr/lib/os-release
COPY --from=base /var/lib /var/spool /var/run /var/log /var/lock /var/local /var/cache /var/backups /var/
COPY --from=base /var/lib/misc /var/lib/misc/
COPY --from=base /var/lib/dpkg/status.d/tzdata* \
/var/lib/dpkg/status.d/libc6* \
/var/lib/dpkg/status.d/libgcc-s1* \
/var/lib/dpkg/status.d/base-files* /var/lib/dpkg/status/


# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
