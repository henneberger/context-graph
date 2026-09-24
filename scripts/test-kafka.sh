#!/usr/bin/env bash
# Start an isolated single-broker integration fixture, separate from Kubernetes Kafka.
set -euo pipefail
name=context-graph-kafka-test
if docker inspect "$name" >/dev/null 2>&1; then
  echo "Container $name already exists; reuse it or remove that test fixture explicitly."
  exit 1
fi
docker run -d --name "$name" --label context-graph.fixture=true -p 127.0.0.1:19092:19092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9092,EXTERNAL://localhost:19092 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=3 \
  -e 'KAFKA_HEAP_OPTS=-Xmx384m -Xms256m' apache/kafka:4.1.1
for attempt in {1..30}; do
  if docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo 'Integration broker ready on localhost:19092'
    exit 0
  fi
  sleep 1
done
echo 'Integration broker failed to become ready' >&2
exit 1
