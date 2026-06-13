# Spring Boot Kafka Microservices

This project is a Spring Boot application that utilizes Apache Kafka for asynchronous messaging between services. The architecture consists of two microservices: **User Service** and **User Address Service**, which communicate through Kafka topics. Additionally, there is a **Notification Service** that listens for events and sends notifications accordingly.

## Architecture Overview

- **User Service**: Manages user-related operations and sends user events to Kafka topics.
- **User Address Service**: Consumes user events from Kafka topics to manage user addresses.
- **Notification Service**: Consumes events from Kafka topics to send notifications based on user actions.

Both the **User Address Service** and **Notification Service** listen to the same Kafka topic, allowing them to respond to user creation events in real-time.

## Features

- Event-driven architecture using Kafka for high-throughput data streams.
- Asynchronous communication between microservices.
- Real-time notifications for user-related events.

## Technologies Used

- Spring Boot
- JDK 17
- Apache Kafka
- Spring Kafka
- Postgresql
- Couchbase
- Docker

## DIFC + SASL workflow (difc-for-kafka)

For a secured end-to-end demo (KRaft cluster, SCRAM principals, DIFC tags `user-contact` / `user-shipping` on `user-service.user_created.1`), see:

```bash
cd config/security
chmod +x *.sh
./run-workflow-from-scratch.sh
```

Logs: `/tmp/sbk-workflow-logs/`. Details: [config/security/README.md](config/security/README.md).

Requires DIFC-enabled `kafka-clients` 4.0.0 in local Maven (`difc-for-kafka` `./gradlew publishToMavenLocal`) and JDK 21+.

## Getting Started (original Docker demo)

### Prerequisites

- JDK 17+
- Gradle 
- Docker

### Setup

1. **Clone the repository**:

   ```bash
   git clone <https://github.com/nihadamirov/spring-boot-kafka>
   cd spring-kafka-microservices
   ```

2. **Run Docker containers**:
   ```bash
   docker-compose up
   ```

3. **Build the application**:

   Navigate to each microservice directory (`user-service`, `user-address-service`, and `notification-consumer`) and run:

   ```bash
   ./gradlew build
   ```

## Communication Flow

1. **User Service** publishes events to Kafka when user actions occur.
2. **User Address Service** consumes these events to manage user addresses accordingly.
3. **Notification Service** consumes events to send notifications to users.
4. Both **User Address Service** and **Notification Service** listen to the same Kafka topic, ensuring that both services can react to user creation events.


## Additional Resources
- [Apache Kafka vs RabbitMQ](https://medium.com/@mehmetbaz/apache-kafka-vs-rabbitmq-82128c579e66)
- [What is Apache Kafka?](https://aws.amazon.com/what-is/apache-kafka/)
- [The Data Streaming Landscape 2024](https://www.kai-waehner.de/blog/2023/12/21/the-data-streaming-landscape-2024/)
- [Understanding Apache Kafka: A Deep Dive Into Its Architecture](https://medium.com/@buttraheel6/understanding-apache-kafka-a-deep-dive-into-its-architecture-9fed765507a3)
- [Spring Kafka Guide](https://www.baeldung.com/spring-kafka)
- [Spring Kafka Reference Documentation](https://docs.spring.io/spring-kafka/reference/kafka.html)

