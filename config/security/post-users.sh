#!/usr/bin/env bash
# POST sample users to user-service REST API.
set -euo pipefail

PORT="${USER_SERVICE_PORT:-8801}"
BASE="http://localhost:${PORT}/api/user"
COUNT="${1:-3}"

post_user() {
  local first="$1"
  local last="$2"
  local email="$3"
  local address="$4"
  curl -sS -X POST "${BASE}" \
    -H 'Content-Type: application/json' \
    -d "{\"firstName\":\"${first}\",\"lastName\":\"${last}\",\"email\":\"${email}\",\"addressText\":\"${address}\"}"
  echo ""
}

echo "Posting ${COUNT} users to ${BASE}"

post_user "Alice" "Smith" "alice@example.com" "1 Main St"
post_user "Bob" "Jones" "bob@example.com" "2 Oak Ave"
post_user "Admin" "User" "admin@corp.com" "3 Secret Rd"

if [[ "${COUNT}" -gt 3 ]]; then
  for ((i = 4; i <= COUNT; i++)); do
    post_user "User${i}" "Test" "user${i}@example.com" "${i} Test Lane"
  done
fi

echo "Done posting users."
