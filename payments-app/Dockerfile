ARG GG_IMAGE=dr-gridgain:9.2.0-b1
FROM ${GG_IMAGE} AS build
USER root
COPY src /src
RUN mkdir /out && /opt/java/openjdk/bin/javac -d /out -cp "/opt/gridgain/lib/*" $(find /src -name '*.java')

FROM ${GG_IMAGE}
COPY --from=build /out /app
ENTRYPOINT ["java", "-Xmx300m", "-XX:+UseSerialGC", "--enable-native-access=ALL-UNNAMED", "-cp", "/app:/opt/gridgain/lib/*", "demo.PaymentsApi"]
