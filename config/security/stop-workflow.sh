#!/usr/bin/env bash
# Stop spring-boot-kafka DIFC workflow processes (Kafka + Spring Boot).
set -euo pipefail

echo "Stopping Spring Boot services..."
pkill -f 'user-service-0.0.1-SNAPSHOT.jar' 2>/dev/null || true
pkill -f 'user-address-service-0.0.1-SNAPSHOT.jar' 2>/dev/null || true
pkill -f 'notification-consumer-0.0.1-SNAPSHOT.jar' 2>/dev/null || true

echo "Stopping Kafka (controller + brokers)..."
pkill -f 'spring-boot-kafka/config/security/controller.properties' 2>/dev/null || true
pkill -f 'spring-boot-kafka/config/security/broker.node1.properties' 2>/dev/null || true
pkill -f 'spring-boot-kafka/config/security/broker.node2.properties' 2>/dev/null || true

sleep 3

for port in 8801 8802 8803 9092 9094 9093; do
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
  fi
done

sleep 2

if ss -tln 2>/dev/null | grep -qE ':9092 |:9094 |:9093 |:8801 |:8802 |:8803 '; then
  echo "Some ports still in use. Remaining listeners:"
  ss -tln | grep -E ':9092|:9094|:9093|:8801|:8802|:8803' || true
  echo "If needed, stop manually with Ctrl+C in each terminal, or:"
  echo "  pgrep -af 'broker.node|controller.properties|user-service-0.0.1'"
  echo "  kill <pid>"
  exit 1
fi

echo "All workflow processes stopped."
