# DIFC grant and data-flow status — spring-boot-kafka

Generated from workflow logs under `/tmp/sbk-workflow-logs`.

## spring-boot-kafka
Logs: `/tmp/sbk-workflow-logs` (present)

### Data flow
| Metric | Value |
|--------|-------|
| orders/users posted | ? |
| validated events | 0 |
| failed events | 0 |
| aggregation decisions | 0 |

### Capability grants (grantor decision)
| Grantor | Requester | Tag | CAN_ADD | CAN_REMOVE | Notes |
|--------|-----------|-----|---------|------------|-------|
| user-svc | notification-svc | user-contact | GRANTED | - |  |
| user-svc | user-address-svc | user-shipping | GRANTED | - |  |

### External connections
| Principal | Target | Allowed | Expected? |
|-----------|--------|---------|-----------|
| notification-svc | - | - | OK (broker-only) |
| user-address-svc | - | - | OK (broker-only) |
| user-svc | - | - | OK (broker-only) |
