#!/usr/bin/env bash
# Wipe, format, start Kafka + Spring Boot services, post users, print log summary.
set -euo pipefail
source "$(dirname "$0")/_env.sh"

LOG_ROOT="${LOG_ROOT:-/tmp/sbk-workflow-logs}"
STATE_DIR="${STATE_DIR:-/tmp/sbk-kafka-demo}"
export STATE_DIR LOG_ROOT
mkdir -p "${LOG_ROOT}" "${STATE_DIR}/policy"
rm -rf "${STATE_DIR}/policy"/*
: > "${LOG_ROOT}/pids.txt"

kill_listeners() {
  echo "Stopping prior demo processes..."
  for port in 9092 9094 9093 8801 8802 8803; do
    if command -v fuser >/dev/null 2>&1; then
      fuser -k "${port}/tcp" 2>/dev/null || true
    fi
  done
  pkill -f 'kafka-server-start.*sbk-sasl' 2>/dev/null || true
  pkill -f 'user-service-0.0.1-SNAPSHOT.jar' 2>/dev/null || true
  pkill -f 'user-address-service-0.0.1-SNAPSHOT.jar' 2>/dev/null || true
  pkill -f 'notification-consumer-0.0.1-SNAPSHOT.jar' 2>/dev/null || true
  sleep 2
}

wait_for_port() {
  local port="$1"
  local label="$2"
  local tries="${3:-60}"
  for ((n = 1; n <= tries; n++)); do
    if ss -tln 2>/dev/null | grep -q ":${port} "; then
      echo "${label} listening on :${port}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${label} did not start on :${port}" >&2
  return 1
}

count_in_logs() {
  local pattern="$1"
  shift
  grep -hE "${pattern}" "$@" 2>/dev/null | wc -l | tr -d '[:space:]'
}

wait_for_kafka() {
  local tries="${1:-90}"
  for ((n = 1; n <= tries; n++)); do
    if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_CONFIG}" >/dev/null 2>&1; then
      echo "Kafka cluster reachable at ${BOOTSTRAP_SERVERS}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Kafka cluster not reachable" >&2
  return 1
}

wait_for_user_http() {
  local tries="${1:-90}"
  for ((n = 1; n <= tries; n++)); do
    if curl -sf "http://localhost:8801/actuator/health" >/dev/null 2>&1 \
      || grep -q '\[UserService\] DIFC bootstrap complete' "${LOG_ROOT}/user-service.log" 2>/dev/null; then
      echo "user-service is up"
      return 0
    fi
    if ss -tln 2>/dev/null | grep -q ':8801 '; then
      if grep -q 'Started UserServiceApplication' "${LOG_ROOT}/user-service.log" 2>/dev/null; then
        echo "user-service is up"
        return 0
      fi
    fi
    sleep 2
  done
  echo "ERROR: user-service did not become ready" >&2
  tail -30 "${LOG_ROOT}/user-service.log" 2>/dev/null || true
  return 1
}

start_bg() {
  local name="$1"
  shift
  local log="${LOG_ROOT}/${name}.log"
  echo "Starting ${name} -> ${log}"
  nohup "$@" >"${log}" 2>&1 &
  echo "${name}:$!" >> "${LOG_ROOT}/pids.txt"
}

kill_listeners

echo "=== Building services ==="
./build-all.sh | tee "${LOG_ROOT}/build.log"

echo "=== Wiping and formatting cluster ==="
./wipe-cluster-logs.sh
./format-kraft-storage.sh | tee "${LOG_ROOT}/format.log"

echo "=== Starting Kafka ==="
start_bg controller "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONTROLLER_CONFIG}"
wait_for_port 9093 controller 60
start_bg broker-1 env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER1_CONFIG}"
start_bg broker-2 env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER2_CONFIG}"
wait_for_port 9092 broker-1 90
wait_for_port 9094 broker-2 90
wait_for_kafka 90

echo "=== ACLs and topics ==="
./create-acls.sh | tee "${LOG_ROOT}/create-acls.log"
./create-topics.sh | tee "${LOG_ROOT}/create-topics.log"

echo "=== Starting Spring Boot services (user-service first for DIFC tags) ==="
start_bg user-service ./start-user-service.sh
wait_for_user_http 120

start_bg user-address-service ./start-user-address-service.sh
start_bg notification-consumer ./start-notification-consumer.sh
sleep 10

echo "Waiting for DIFC GRANT_CAP grants (user-service poll handler)..."
for ((n = 1; n <= 90; n++)); do
  grants=$(grep -c '\[DIFC\] grantPrivilege' "${LOG_ROOT}/user-service.log" 2>/dev/null || true)
  if [[ "${grants}" -ge 2 ]]; then
    echo "DIFC grantPrivilege via poll handler (${grants} approvals)"
    break
  fi
  sleep 2
done

wait_for_consumer() {
  local name="$1"
  local pattern="$2"
  for ((n = 1; n <= 60; n++)); do
    if grep -q "${pattern}" "${LOG_ROOT}/${name}.log" 2>/dev/null; then
      echo "${name} is consuming"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${name} did not start consuming" >&2
  tail -20 "${LOG_ROOT}/${name}.log" 2>/dev/null || true
  return 1
}

wait_for_consumer user-address-service 'DIFC labels applied'
wait_for_consumer notification-consumer 'DIFC labels applied'

wait_for_partitions() {
  local name="$1"
  for ((n = 1; n <= 90; n++)); do
    if grep -q 'partitions assigned' "${LOG_ROOT}/${name}.log" 2>/dev/null; then
      echo "${name} has partition assignment"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${name} never received partition assignment" >&2
  return 1
}

wait_for_partitions user-address-service
wait_for_partitions notification-consumer
sleep 3

echo "=== Posting users (after consumers are assigned) ==="
./post-users.sh 3 | tee "${LOG_ROOT}/post-users.log"

# Ensure all six tagged records are on the broker before checking consumption.
for ((n = 1; n <= 90; n++)); do
  send_ok=$(grep -c 'sendWithTags OK' "${LOG_ROOT}/user-service.log" 2>/dev/null || true)
  if [[ "${send_ok}" -ge 6 ]]; then
    echo "All producer records acknowledged (${send_ok})"
    break
  fi
  sleep 2
done

echo "=== Waiting for producer acks and consumption ==="
for ((n = 1; n <= 60; n++)); do
  send_ok=$(grep -c 'sendWithTags OK' "${LOG_ROOT}/user-service.log" 2>/dev/null || true)
  consumed=$(count_in_logs '^\[(AddressService|NotificationService)\] (Consumed user-|Skipping record)' \
    "${LOG_ROOT}/user-address-service.log" "${LOG_ROOT}/notification-consumer.log")
  if [[ "${send_ok}" -ge 6 && "${consumed}" -ge 6 ]]; then
    echo "Producer and consumers verified (sendWithTags OK=${send_ok}, consume/skip=${consumed})"
    break
  fi
  sleep 2
done
sleep 5

echo "=== Log summary ==="
summarize() {
  local file="$1"
  local pattern="$2"
  echo ""
  echo "---- ${file} (${pattern}) ----"
  grep -E "${pattern}" "${LOG_ROOT}/${file}.log" 2>/dev/null | tail -25 || echo "(no matches)"
}

summarize user-service '\[UserService\]|\[DIFC\]|user-contact|user-shipping|sendWithTags'
summarize user-address-service '\[AddressService\]|\[DIFC\]|user-shipping|Skipping|Saved address'
summarize notification-consumer '\[NotificationService\]|\[DIFC\]|user-contact|Skipping|welcome'
summarize broker-1 'DIFC|ADD_CLIENT_PRIVS|Final message tags|user-contact|user-shipping'
summarize broker-2 'DIFC|ADD_CLIENT_PRIVS|Final message tags|user-contact|user-shipping'

echo ""
echo "=== Verification ==="
FAIL=0
send_ok=$(grep -c 'sendWithTags OK' "${LOG_ROOT}/user-service.log" 2>/dev/null || true)
# Count stdout lines only (avoid double-counting log.info duplicates).
contact_ok=$(grep -c '^\[NotificationService\] Consumed user-contact' "${LOG_ROOT}/notification-consumer.log" 2>/dev/null || true)
shipping_ok=$(grep -c '^\[AddressService\] Consumed user-shipping' "${LOG_ROOT}/user-address-service.log" 2>/dev/null || true)
skip_contact=$(grep -c '^\[AddressService\] Skipping record.*user-contact' "${LOG_ROOT}/user-address-service.log" 2>/dev/null || true)
skip_shipping=$(grep -c '^\[NotificationService\] Skipping record.*user-shipping' "${LOG_ROOT}/notification-consumer.log" 2>/dev/null || true)
saved_ok=$(grep -c '^\[AddressService\] Saved address' "${LOG_ROOT}/user-address-service.log" 2>/dev/null || true)

echo "sendWithTags OK: ${send_ok} (expect >= 6)"
echo "notification consumed user-contact: ${contact_ok} (expect >= 3)"
echo "address consumed user-shipping: ${shipping_ok} (expect >= 3)"
echo "address skipped user-contact: ${skip_contact} (expect >= 3)"
echo "notification skipped user-shipping: ${skip_shipping} (expect >= 3)"
echo "address saved: ${saved_ok} (expect >= 3)"

[[ "${send_ok}" -ge 6 ]] || FAIL=1
[[ "${contact_ok}" -ge 3 ]] || FAIL=1
[[ "${shipping_ok}" -ge 3 ]] || FAIL=1
[[ "${skip_shipping}" -ge 3 ]] || FAIL=1
[[ "${saved_ok}" -ge 3 ]] || FAIL=1

echo ""
echo "Full logs under ${LOG_ROOT}"
if [[ "${FAIL}" -ne 0 ]]; then
  echo "Workflow verification FAILED — see counts above." >&2
  exit 1
fi
echo "Workflow run complete — DIFC access control verified."
