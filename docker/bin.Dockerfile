
# our final base
FROM debian:bookworm-slim

# non-root user
ARG USERNAME=iceberg
ARG USER_UID=5001
ARG USER_GID=$USER_UID
ARG BIN=./target/release/iceberg-catalog

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
COPY ${BIN} ./iceberg-catalog

# # set the startup command to run your binary
CMD ["./iceberg-catalog"]
