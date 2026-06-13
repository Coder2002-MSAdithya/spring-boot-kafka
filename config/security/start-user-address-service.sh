#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
JAR="${PROJECT_HOME}/user-address-service/build/libs/user-address-service-0.0.1-SNAPSHOT.jar"
exec java -jar "${JAR}" \
  --spring.profiles.active=workflow \
  --spring.config.additional-location="file:${SCRIPT_DIR}/user-address-service.properties"
