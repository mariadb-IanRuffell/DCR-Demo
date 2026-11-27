# GridGain 9.2 node image for the demo.
# The columnar storage engine ships a musl/aarch64 JNI library that segfaults on load under
# Docker Desktop (Apple silicon). The demo only uses aipersist, so the jar is removed.
ARG GG_IMAGE=gridgain/gridgain9:9.2.0-b1
FROM ${GG_IMAGE}
USER root
RUN rm -f /opt/gridgain/lib/gridgain-columnar-*.jar
USER gridgain
