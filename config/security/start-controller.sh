#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONTROLLER_CONFIG}"
