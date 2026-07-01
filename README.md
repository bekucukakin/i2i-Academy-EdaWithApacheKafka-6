# i2i-Academy-EdaWithApacheKafka-6

### Event-Driven Architecture with Apache Kafka

A hands-on implementation of Event-Driven Architecture (EDA) principles using
Apache Kafka. A Kafka broker runs via Docker Compose; a Java Producer
application publishes custom `OrderEvent` objects to a topic, and a Consumer
application reads and prints them to the console.

## Architecture

- **Broker:** Apache Kafka (KRaft mode, single broker via Docker Compose)
- **Producer:** Serializes `OrderEvent` objects to JSON and publishes them to
  the `order-events` topic.
- **Consumer:** Subscribes to the `order-events` topic, deserializes the
  incoming JSON back into `OrderEvent` objects, and prints them to the
  console.

## Requirements

- Docker & Docker Compose
- Java 17+
- Maven 3.8+

## Running

```bash
# 1) Start Kafka
docker compose up -d

# 2) Create the topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic order-events --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1

# 3) Start the consumer (separate terminal)
mvn exec:java -Dexec.mainClass="com.i2iacademy.kafka.consumer.ConsumerApp"

# 4) Start the producer (another terminal)
mvn exec:java -Dexec.mainClass="com.i2iacademy.kafka.producer.ProducerApp"
```

## Project Structure

```
src/main/java/com/i2iacademy/kafka/
├── model/      OrderEvent domain object
├── serde/      Custom Kafka Serializer/Deserializer
├── producer/   ProducerApp
└── consumer/   ConsumerApp
```
