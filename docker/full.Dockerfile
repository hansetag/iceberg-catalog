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
FROM gcr.io/distroless/cc-debian12:nonroot as base


FROM scratch as final
ARG EXPIRES=Never
LABEL maintainer="moderation@hansetag.com" quay.expires-after=${EXPIRES}

COPY --from=base /home /home
COPY --from=base /etc/ /etc/
COPY --from=base /lib/ /lib/
COPY --from=base /usr/share /usr/share
COPY --from=base /usr/lib/os-release /usr/lib/os-release
COPY --from=base /sys /sys
COPY --from=base /proc /proc
COPY --from=base /boot /boot
COPY --from=base /bin /bin
COPY --from=base /dev /dev
COPY --from=base /root /root
COPY --from=base /tmp /tmp
COPY --from=base /var/lib/dpkg/status.d/tzdata.md5sums /var/lib/dpkg/status.d/
COPY --from=base /var/lib/dpkg/status.d/libc6.md5sums /var/lib/dpkg/status.d/
COPY --from=base /var/lib/dpkg/status.d/libgcc-s1 /var/lib/dpkg/status.d/
COPY --from=base /var/lib/dpkg/status.d/libgcc-s1.md5sums /var/lib/dpkg/status.d/
COPY --from=base /var/lib/dpkg/status.d/base-files.md5sums /var/lib/dpkg/status.d/
COPY --from=base /var/lib/dpkg/status.d/base-files /var/lib/dpkg/status.d/



# copy the build artifact from the build stage
COPY --from=builder /app/target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
