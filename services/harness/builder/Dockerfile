# Build the three harness runtime images first (see services/harness/README.md).
FROM context-graph/query:harness-v3 AS query
FROM context-graph/processor:harness-v2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /platform
COPY services/harness/requirements.txt /platform/services/harness/requirements.txt
RUN python3 -m venv /opt/harness && /opt/harness/bin/pip install --no-cache-dir -r /platform/services/harness/requirements.txt
COPY --from=query /app/query.jar /platform/query.jar
COPY services/harness /platform/services/harness
COPY scripts/cg /platform/scripts/cg
COPY examples/group-insights /platform/examples/group-insights
COPY config /platform/config
COPY deploy/k8s /platform/deploy/k8s
COPY .build/ax-task-runner /usr/local/bin/ax-task-runner
ENV PATH="/opt/harness/bin:/platform/scripts:${PATH}" CG_PROCESSOR_CLASSPATH="/opt/flink/usrlib/processor.jar:/opt/flink/lib/*" CG_QUERY_CLASSPATH="/platform/query.jar"
RUN mkdir -p /workspace && chown -R flink:flink /workspace /platform
# AX expects port 80; the runner contains no Kubernetes client or deployment credentials.
EXPOSE 80
ENTRYPOINT ["/usr/local/bin/ax-task-runner"]
