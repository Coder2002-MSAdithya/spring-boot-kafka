#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
echo "Cluster id: ${CLUSTER_ID}"
echo "${CLUSTER_ID}" > "${SCRIPT_DIR}/.cluster-id"

SCRAM_ARGS=(
  --add-scram 'SCRAM-SHA-256=[name=admin,password=admin-secret]'
  --add-scram 'SCRAM-SHA-256=[name=user-svc,password=user-secret]'
  --add-scram 'SCRAM-SHA-256=[name=user-address-svc,password=user-address-secret]'
  --add-scram 'SCRAM-SHA-256=[name=notification-svc,password=notification-secret]'
)

echo "Formatting controller (node 1)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --standalone -t "${CLUSTER_ID}" \
  -c "${CONTROLLER_CONFIG}" \
  "${SCRAM_ARGS[@]}"

echo "Formatting broker 1 (node 2)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER1_CONFIG}"

echo "Formatting broker 2 (node 3)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER2_CONFIG}"

echo "Formatting broker 3 (node 4)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER3_CONFIG}"

echo "Formatting broker 4 (node 5)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER4_CONFIG}"

echo "Done. Start: start-controller.sh → start-broker-1.sh → start-broker-2.sh → start-broker-3.sh → start-broker-4.sh"
