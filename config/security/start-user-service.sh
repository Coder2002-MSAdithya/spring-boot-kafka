#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
source "$(dirname "$0")/_policy_agent.sh"
JAR="${PROJECT_HOME}/user-service/build/libs/user-service-0.0.1-SNAPSHOT.jar"
readarray -t AGENT_OPTS < <(policy_agent_java_opts user-svc UserServiceApplication "${STATE_DIR}")
readarray -t GRANTOR_OPTS < <(grantor_policy_trust_java_opts "${STATE_DIR}")
exec java "${GRANTOR_OPTS[@]}" "${AGENT_OPTS[@]}" -jar "${JAR}" \
  --spring.profiles.active=workflow \
  --spring.config.additional-location="file:${SCRIPT_DIR}/user-service.properties"
