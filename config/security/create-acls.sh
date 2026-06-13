#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
ACL_BIN="${KAFKA_HOME}/bin/kafka-acls.sh"

run_acl() {
  "${ACL_BIN}" --bootstrap-server "${BS}" --command-config "${ADMIN_CONFIG}" "$@"
}

USERS=(
  user-svc
  user-address-svc
  notification-svc
)

for user in "${USERS[@]}"; do
  echo "ACLs for User:${user}"
  run_acl --add --allow-principal "User:${user}" --operation All --cluster
  run_acl --add --allow-principal "User:${user}" --operation All \
    --topic '*' --resource-pattern-type literal
  run_acl --add --allow-principal "User:${user}" --operation All \
    --group '*' --resource-pattern-type literal
  run_acl --add --allow-principal "User:${user}" --operation All \
    --transactional-id '*' --resource-pattern-type literal
done

echo "ACLs created."
