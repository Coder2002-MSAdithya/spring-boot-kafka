#!/usr/bin/env bash
set -euo pipefail

DIRS=(
  /tmp/kafka-logs-sbk-sasl-controller
  /tmp/kafka-logs-sbk-sasl-broker-1
  /tmp/kafka-logs-sbk-sasl-broker-2
  /tmp/kafka-logs-sbk-sasl-broker-3
  /tmp/kafka-logs-sbk-sasl-broker-4
)

for d in "${DIRS[@]}"; do
  if [[ -d "${d}" ]]; then
    echo "Removing ${d}"
    rm -rf "${d}"
  fi
done

echo "Wiped spring-boot-kafka KRaft log directories."
