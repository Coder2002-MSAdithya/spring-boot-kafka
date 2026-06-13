#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
exec env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER1_CONFIG}"
