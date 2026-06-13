#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION_FACTOR:-2}"
TOPICS_BIN="${KAFKA_HOME}/bin/kafka-topics.sh"
TOPIC="user-service.user_created.1"

echo "Bootstrap: ${BS}"
echo "Creating topic ${TOPIC}: partitions=${PARTITIONS} rf=${REPLICATION}"
"${TOPICS_BIN}" --bootstrap-server "${BS}" \
  --command-config "${ADMIN_CONFIG}" \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}"

"${TOPICS_BIN}" --bootstrap-server "${BS}" \
  --command-config "${ADMIN_CONFIG}" \
  --list | grep -F 'user-service.user_created.1' || true

echo "Done."
