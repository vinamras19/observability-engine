# Distributed Observability Engine

A distributed telemetry pipeline designed to ingest, aggregate, and analyze server metrics in real-time. This project implements a Kafka Streams topology for stateful windowed aggregation, a signal processing layer for statistical anomaly detection, and a custom InfluxDB sink for persistent time-series storage.

## Technology Stack

* **Core Application:** Java 17, Maven
* **Stream Processing:** Apache Kafka (Kafka Streams API)
* **Time-Series Database:** InfluxDB v2
* **Visualization:** Grafana
* **Infrastructure:** Docker, Docker Compose, Zookeeper
* **Testing:** JUnit 5

## Engineering Highlights

* **Signal Analysis:** Applies three statistical techniques to the aggregated metric stream - EWMA smoothing for noise filtering, CUSUM change point detection for identifying sustained mean shifts, and streaming Shannon entropy estimation over a sliding window for detecting regime changes.
* **Fault Tolerance:** Implemented a Poison Pill pattern in the stream topology. Malformed JSON records are logged and discarded at the ingress point to prevent stream thread crashes.
* **Write Optimization:** The Database Sink implements a manual batching mechanism (flushes every 500 records or 5 seconds) to reduce network overhead to InfluxDB.
* **Windowed Analytics:** Uses 60-second tumbling time windows to calculate min, max, and avg CPU usage in real-time.
* **DevEx & Networking:** Configured a dual-listener Kafka setup (PLAINTEXT for internal Docker traffic, PLAINTEXT_HOST for localhost) to allow local Java processes to communicate with containerized infrastructure.
* **Data Simulation:** The metric producer uses a stateful Random Walk algorithm to generate drifting CPU trends for threshold testing.

## System Architecture

The system follows a standard **Producer → Processor → Sink** pattern, decoupled by Kafka topics.

```mermaid
graph LR
    %% Components
    Prod[Metric Producer] -->|JSON| T1[raw-metrics]
    T1 --> Engine[Analytics Engine]
    
    subgraph "Kafka Streams"
        Engine -->|Windowed Aggregation| Engine
        Engine -->|EWMA / CUSUM / Entropy| Signal[Signal Analyzer]
    end
    
    Engine -->|Aggregated Data| T2[analyzed-metrics]
    Signal -->|Anomaly Data| T3[signal-alerts]
    T2 --> Sink[Database Sink]
    T3 --> SSink[Signal Sink]
    
    Sink -->|Batch Write| DB[(InfluxDB)]
    SSink -->|Batch Write| DB
    DB -->|Query| Dash[Grafana]

    %% Styling
    classDef plain fill:#fff,stroke:#333,stroke-width:1px;
    classDef db fill:#f9f9f9,stroke:#333,stroke-width:1px;
    class Prod,Engine,Signal,Sink,SSink,T1,T2,T3,Dash plain;
    class DB db;
```

## Getting Started

### Prerequisites

* Java 17+
* Maven 3.6+
* Docker & Docker Compose

### Build
* Compile the application and run unit tests.
```text
mvn clean package
```
* Start Infrastructure
```text
docker-compose up -d
```
* Initialize Topics

```text
docker exec observability-engine-kafka-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic raw-metrics
docker exec observability-engine-kafka-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic analyzed-metrics
docker exec observability-engine-kafka-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic metric-alerts
docker exec observability-engine-kafka-1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic signal-alerts
```
### Running the Pipeline

To simulate the distributed environment, run the components in separate terminal windows.

Terminal 1: Analytics Engine (The Processor)
```text
java -jar target/observability-engine-1.0.0.jar
```
Terminal 2: Database Sink (The Consumer)
```text
java -cp target/observability-engine-1.0.0.jar com.engine.sink.DatabaseSink
```
Terminal 3: Signal Sink (Signal Analysis Consumer)
```text
java -cp target/observability-engine-1.0.0.jar com.engine.sink.SignalSink
```
Terminal 4: Metric Producer (The Generator)
```text
java -cp target/observability-engine-1.0.0.jar com.engine.producer.MetricProducer
```
## Observability

```text
Grafana Dashboard: http://localhost:3000 (User: admin / Pass: admin)

Visualizes real-time CPU trends, breach counts, and system status.

InfluxDB UI: http://localhost:8086

Tests: Run mvn test to verify aggregation and signal analysis logic.
```

## Dashboard

![Dashboard](dashboard-demo.png)

![Signal Analysis](signal-analysis-demo.png)

## Configuration

```text
Possible Modifications in Analytics Engine:

WINDOW_SECONDS: Aggregation window size (Default: 60s)

ALERT_THRESHOLD: CPU % that triggers a warning (Default: 85.0)
```
```text
Signal Analyzer Defaults (configurable in SignalAnalyzer constructor):

  EWMA alpha:           0.3   (smoothing factor)
  CUSUM k:              0.5   (allowance / slack)
  CUSUM h:              4.0   (decision threshold)
  Entropy window size:  30    (sliding window of recent values)
  Entropy bins:         20    (histogram resolution across 0-100%)
```

## License
See `LICENSE` for more information.