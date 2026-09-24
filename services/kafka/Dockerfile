FROM alpine:3.22 AS exporter
RUN apk add --no-cache curl
RUN curl -fsSL https://github.com/prometheus/jmx_exporter/releases/download/1.6.0/jmx_prometheus_javaagent-1.6.0.jar -o /jmx.jar \
 && echo "a95983fd96e865d2bcdf911cc500e7c82808c27ab9fd226bf96732b6c3d8c46e  /jmx.jar" | sha256sum -c -
FROM apache/kafka:4.1.1
COPY --from=exporter /jmx.jar /opt/metrics/jmx.jar
COPY services/kafka/metrics.yaml /opt/metrics/config.yaml
ENV KAFKA_OPTS="-javaagent:/opt/metrics/jmx.jar=9404:/opt/metrics/config.yaml"
