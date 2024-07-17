ARG ARCH

FROM gcr.io/distroless/cc-debian12:nonroot-${ARCH}
ARG BIN

# copy the build artifact from the build stage
COPY ${BIN} /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
