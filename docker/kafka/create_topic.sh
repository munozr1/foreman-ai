#!/bin/sh
set -eu
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic "${KAFKA_TOPIC:-fdb.cdc.foremen}" --partitions 1 --replication-factor 1
