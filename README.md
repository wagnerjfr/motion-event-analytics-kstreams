# motion-event-analytics-kstreams

Kafka Streams application that consumes motion position and collision events from Kafka and computes per-ball analytics in real time.

## Pipeline

```
motion-position ──→ PositionEvent ──→ MetricUpdate ──┐
                                                       ├── merge → groupByKey → aggregate → toStream → map → motion-ball-analytics
motion-collision ──→ CollisionEvent ──→ MetricUpdate ─┘
                                                       │
                                            KeyValueStore (analytics-store)
```

For each ball (keyed by `sessionId|ballId`), the app tracks:

- **Latest position** (x, y) and **current speed**
- **Average speed** over a 10-second sliding window
- **Collision count** over a 30-second sliding window

## Prerequisites

- Java 21
- Docker (for Kafka)
- Kafka broker running on the `kafka-net` Docker network
- Topics `motion-position` and `motion-collision` created

Start Kafka:

```bash
docker network create kafka-net   # one-time
docker run -d --name kafka --network kafka-net -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT_HOST://:9092,PLAINTEXT_DOCKER://:29092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9092,PLAINTEXT_DOCKER://kafka:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT_DOCKER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_DOCKER \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:3.8.0
```

Create topics:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic motion-position --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic motion-collision --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic motion-ball-analytics --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

## Producer

Clone, build, and run the [motion-event-producer](https://github.com/wagnerjfr/motion-event-producer) to publish events to Kafka (requires Java 17+ and Maven 3.9+):

```bash
git clone git@github.com:wagnerjfr/motion-event-producer.git
cd motion-event-producer
mvn -DskipTests compile
APP_TRANSPORT_MODE=kafka \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
mvn javafx:run
```

This opens a JavaFX physics simulation UI. Click **Start** to begin publishing position and collision events to `motion-position` and `motion-collision`.

## Build

```bash
mvn -DskipTests clean package
```

Output: `target/motion-event-analytics-kstreams.jar`

## Run

### Host mode (direct connection to Kafka)

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
java -jar target/motion-event-analytics-kstreams.jar
```

### Docker mode (container on kafka-net)

```bash
docker build -t motion-event-analytics-kstreams .
docker run -d --name motion-analytics --network kafka-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  motion-event-analytics-kstreams
```

## Verify

With the producer running and sending events, consume the output topic to confirm the stream is processing:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic motion-ball-analytics --bootstrap-server localhost:9092 \
  --property print.key=true --from-beginning
```

Each record shows a composite key (`sessionId|ballId`) and a JSON value with current speed, 10s avg speed, and collision count over 30s.

You can also browse the topic via **AKHQ** at `http://localhost:8081` (started as part of the producer setup).

### Required startup order

1. Start Kafka + create topics (`motion-position`, `motion-collision`)
2. Build & start this analytics app
3. Clone, build & start the [motion-event-producer](https://github.com/wagnerjfr/motion-event-producer)
4. Verify via console consumer or AKHQ

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_POSITION` | `motion-position` | Position events topic |
| `KAFKA_TOPIC_COLLISION` | `motion-collision` | Collision events topic |
| `KAFKA_TOPIC_ANALYTICS` | `motion-ball-analytics` | Analytics output topic |

## Input event formats

### Position event (`motion-position`)

```json
{
  "sessionId": "session-20250513T103000-1",
  "ballId": "ball-5",
  "timestampMs": 1747123456789,
  "x": 3.421,
  "y": 5.187,
  "vx": 1.234,
  "vy": -0.567
}
```

### Collision event (`motion-collision`)

```json
{
  "sessionId": "session-20250513T103000-1",
  "ballAId": "ball-5",
  "ballBId": "ball-12",
  "timestampMs": 1747123456789,
  "x": 4.0,
  "y": 5.0,
  "relativeSpeed": 2.8
}
```

## Output format (`motion-ball-analytics`)

```json
{
  "sessionId": "session-20250513T103000-1",
  "ballId": "ball-5",
  "timestampMs": 1747123456789,
  "latestX": 3.421,
  "latestY": 5.187,
  "currentSpeed": 1.36,
  "avgSpeed10s": 1.42,
  "collisionsCount30s": 3
}
```

## Architecture

| Component | Framework |
|---|---|
| Runtime | Spring Boot 3.3.5 / Kafka Streams 3.9.0 |
| State store | Persistent `KeyValueStore` (Jackson/JSON serialization) |
| JSON | Jackson 2.x |
| Packaging | Maven + spring-boot-maven-plugin (fat JAR) |

## Kafka Streams vs Flink

This app is the Kafka Streams counterpart of [motion-event-analytics-flink](https://github.com/wagnerjfr/motion-event-analytics-flink), which implements the same pipeline with Flink DataStream.

| Aspect | Kafka Streams (this repo) | Flink DataStream |
|---|---|---|
| Runtime | Standalone JVM (Spring Boot) | Flink cluster (Session / Application mode) |
| Cluster dependency | None | JobManager + TaskManager required |
| State store | Persistent `KeyValueStore` (embedded) | State backend (RocksDB / Heap / Filesystem) |
| Windowing | Manual sliding window via state trimming | Built-in `SlidingEventTimeWindows` |
| JSON handling | StringSerde + manual parsing in `flatMap` | Custom `SerializationSchema` / `TypeInformation` |
| Deployment | `java -jar` or Docker | `flink run` + cluster management |
| Scaling | Single-process, partition-parallel within JVM | Distributed across TaskManagers |
