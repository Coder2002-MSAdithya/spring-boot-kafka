# Spring Boot Kafka — SASL/SCRAM + DIFC workflow

Three Spring Boot services run against **difc-for-kafka** with per-service SCRAM principals. On each user registration, **user-service** publishes **two tagged records** on `user-service.user_created.1`; each consumer only reads the slice it is allowed to see.

## DIFC model

| Tag | Payload | Consumer | Data exposed |
|-----|---------|----------|--------------|
| `user-contact` | `UserContactEvent` | notification-consumer | userId, firstName, lastName, email |
| `user-shipping` | `UserShippingEvent` | user-address-service | userId, addressText |

**user-service** owns both tags and **polls `POLL_PRIVS_REQ`** to grant queued requests when `DifcGrantPolicy` allows the requester/tag/capability triple.

Downstream services send **`GRANT_CAP`** at startup on the **same shared `KafkaConsumer`** used by `@KafkaListener` (via `SharedDifcConsumerFactory`):

| Requester | Tag | CAN_ADD | CAN_REMOVE |
|-----------|-----|---------|------------|
| `notification-svc` | `user-contact` | yes | no (consume only) |
| `user-address-svc` | `user-shipping` | yes | no (consume only) |

Records tagged for the other service appear as **redacted** (null payload) and are skipped in logs.

## Cluster layout

| JVM | Role | Client port |
|-----|------|-------------|
| Controller | `controller` | — (`:9093`) |
| Broker 1 | `broker` | SASL `:9092` |
| Broker 2 | `broker` | SASL `:9094` |

**Bootstrap:** `localhost:9092,localhost:9094`

## Service principals

| Service | SCRAM user | HTTP port |
|---------|------------|-----------|
| user-service | `user-svc` | 8801 |
| user-address-service | `user-address-svc` | 8802 |
| notification-consumer | `notification-svc` | 8803 |

## Prerequisites

- `difc-for-kafka` at `../difc-for-kafka` (or `KAFKA_HOME`)
- DIFC-enabled `kafka-clients` 4.0.0 in `~/.m2/repository`
- JDK 21+

## Build and run

```bash
cd spring-boot-kafka/config/security
chmod +x *.sh
./run-workflow-from-scratch.sh
```

Logs: `/tmp/sbk-workflow-logs/`

## Expected log patterns

- **user-service:** `Publishing user-contact` and `Publishing user-shipping` (two records per POST)
- **notification-consumer:** `Consumed user-contact` with email; skips `user-shipping` records
- **user-address-service:** `Consumed user-shipping` with address; skips `user-contact` records
- **broker:** `Final message tags` with `user-contact` or `user-shipping`
