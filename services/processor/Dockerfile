FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY services/security ./security
RUN mvn -q -f security/pom.xml -DskipTests install
COPY services/processor/pom.xml ./pom.xml
RUN mvn -q dependency:go-offline
COPY services/processor/src ./src
RUN mvn -q -DskipTests package

FROM flink:2.3.0-java21
COPY --from=build /build/target/processor.jar /opt/flink/usrlib/processor.jar
COPY config /app/config
USER root
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
RUN mkdir -p /data/warehouse /data/checkpoints /data/savepoints && chown -R flink:flink /data
# Flink 2.3 bundles the Prometheus reporter under plugins/metrics-prometheus.
USER flink
