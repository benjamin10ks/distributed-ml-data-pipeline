#!/usr/bin/env bash
set -euo pipefail

bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

files_raw_topic="${KAFKA_TOPIC_FILES_RAW:-files.raw}"
files_processed_topic="${KAFKA_TOPIC_FILES_PROCESSED:-files.processed}"
cdc_events_topic="${KAFKA_TOPIC_CDC_EVENTS:-cdc.events}"
dlq_topic="${KAFKA_TOPIC_DLQ:-dead-letter}"

files_raw_partitions="${KAFKA_PARTITIONS_FILES_RAW:-6}"
files_processed_partitions="${KAFKA_PARTITIONS_FILES_PROCESSED:-6}"
cdc_events_partitions="${KAFKA_PARTITIONS_CDC_EVENTS:-3}"
dlq_partitions="${KAFKA_PARTITIONS_DLQ:-1}"

files_raw_retention="${KAFKA_RETENTION_FILES_RAW_MS:-604800000}"
files_processed_retention="${KAFKA_RETENTION_FILES_PROCESSED_MS:-2592000000}"
cdc_events_retention="${KAFKA_RETENTION_CDC_EVENTS_MS:-604800000}"
dlq_retention="${KAFKA_RETENTION_DLQ_MS:-2592000000}"

echo "Waiting for Kafka at ${bootstrap}..."
until kafka-topics --bootstrap-server "${bootstrap}" --list >/dev/null 2>&1; do
  sleep 2
done

create_topic() {
  local topic=$1
  local partitions=$2
  local retention=$3

  kafka-topics --bootstrap-server "${bootstrap}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention}"
}

create_topic "${files_raw_topic}" "${files_raw_partitions}" "${files_raw_retention}"
create_topic "${files_processed_topic}" "${files_processed_partitions}" "${files_processed_retention}"
create_topic "${cdc_events_topic}" "${cdc_events_partitions}" "${cdc_events_retention}"
create_topic "${dlq_topic}" "${dlq_partitions}" "${dlq_retention}"

echo "Kafka topics configured."
