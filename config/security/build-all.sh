#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"

echo "Building difc-common..."
chmod +x "${PROJECT_HOME}/user-service/gradlew"
(cd "${PROJECT_HOME}/difc-common" && "${PROJECT_HOME}/user-service/gradlew" build -x test)

for svc in user-service user-address-service notification-consumer; do
  echo "Building ${svc}..."
  (cd "${PROJECT_HOME}/${svc}" && chmod +x gradlew && ./gradlew build -x test)
done

echo "All services built."
