
ARG ARCH="amd64"
FROM gcr.io/distroless/cc-debian12:nonroot-${ARCH}

# copy the build artifact from the build stage
COPY ./target/release/iceberg-catalog /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
CMD ["/home/nonroot/iceberg-catalog"]
