ARG ARCH

FROM gcr.io/distroless/cc-debian12:nonroot-${ARCH} as base

FROM busybox:1.36.1 as cleaner
# small diversion through busybox to remove some files

COPY --from=base / /clean

RUN rm -r /clean/usr/lib/*-linux-gnu/libgomp*  \
         /clean/usr/lib/*-linux-gnu/libssl*  \
         /clean/usr/lib/*-linux-gnu/libstdc++* \
         /clean/usr/lib/*-linux-gnu/engines-3 \
         /clean/usr/lib/*-linux-gnu/ossl-modules \
         /clean/usr/lib/*-linux-gnu/libcrypto.so.3 \
        /clean/usr/lib/*-linux-gnu/gconv \
       /clean/var/lib/dpkg/status.d/libgomp1*  \
       /clean/var/lib/dpkg/status.d/libssl3*  \
       /clean/var/lib/dpkg/status.d/libstdc++6* \
       /clean/usr/share/doc/libssl3 \
       /clean/usr/share/doc/libstdc++6 \
       /clean/usr/share/doc/libgomp1 \

FROM scratch

ARG BIN
ARG EXPIRES=Never
LABEL maintainer="moderation@hansetag.com" quay.expires-after=${EXPIRES}

COPY --from=cleaner /clean /

# copy the build artifact from the build stage
COPY ${BIN} /home/nonroot/iceberg-catalog

# # set the startup command to run your binary
ENTRYPOINT ["/home/nonroot/iceberg-catalog"]
