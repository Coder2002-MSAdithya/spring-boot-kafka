#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
source "$(dirname "$0")/_policy_agent.sh"
JAR="${PROJECT_HOME}/user-address-service/build/libs/user-address-service-0.0.1-SNAPSHOT.jar"
readarray -t AGENT_OPTS < <(policy_agent_java_opts user-address-svc UserAddressServiceApplication "${STATE_DIR}")
exec java "${AGENT_OPTS[@]}" -jar "${JAR}" \
  --spring.profiles.active=workflow \
  --spring.config.additional-location="file:${SCRIPT_DIR}/user-address-service.properties"
