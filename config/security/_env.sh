#!/usr/bin/env bash
# Shared paths for spring-boot-kafka SASL/DIFC workflow scripts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-$(cd "${PROJECT_HOME}/../difc-for-kafka" && pwd)}"

CONTROLLER_CONFIG="${SCRIPT_DIR}/controller.properties"
BROKER1_CONFIG="${SCRIPT_DIR}/broker.node1.properties"
BROKER2_CONFIG="${SCRIPT_DIR}/broker.node2.properties"
JAAS_CONFIG="${SCRIPT_DIR}/kafka_server_jaas.conf"
ADMIN_CONFIG="${SCRIPT_DIR}/admin-client.properties"

export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092,localhost:9094}"
export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}"

DIFC_COMMON_JAR="${PROJECT_HOME}/difc-common/build/libs/difc-common-0.1.0.jar"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-storage.sh" ]]; then
  echo "ERROR: difc-for-kafka not found at ${KAFKA_HOME}" >&2
  exit 1
fi

require_built_services() {
  for svc in user-service user-address-service notification-consumer; do
    local jar="${PROJECT_HOME}/${svc}/build/libs/${svc}-0.0.1-SNAPSHOT.jar"
    if [[ ! -f "${jar}" ]]; then
      echo "ERROR: Missing ${jar}" >&2
      echo "Run: cd ${PROJECT_HOME} && ./config/security/build-all.sh" >&2
      exit 1
    fi
  done
}
