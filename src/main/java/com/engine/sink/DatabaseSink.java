package com.engine.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DatabaseSink {

    private static final Logger log = LoggerFactory.getLogger(DatabaseSink.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // Config
    private static final String INFLUX_URL = System.getenv("INFLUXDB_URL") != null ? System.getenv("INFLUXDB_URL") : "http://localhost:8086";
    private static final String INFLUX_TOKEN = System.getenv("INFLUXDB_TOKEN") != null ? System.getenv("INFLUXDB_TOKEN") : "my-token";
    private static final String INFLUX_ORG = "engine-org";
    private static final String INFLUX_BUCKET = "telemetry";
    private static final String BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String INPUT_TOPIC = "analyzed-metrics";

    // Batching Config
    private static final int BATCH_SIZE = 500;
    private static final Duration FLUSH_INTERVAL = Duration.ofSeconds(5);

    public static void main(String[] args) {
        log.info("Starting Database Sink service...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "influx-sink-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Influx Client
        InfluxDBClient influxClient = InfluxDBClientFactory.create(INFLUX_URL, INFLUX_TOKEN.toCharArray(), INFLUX_ORG, INFLUX_BUCKET);
        WriteApiBlocking writeApi = influxClient.getWriteApiBlocking();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received via Hook");
            consumer.wakeup(); // The correct way to stop a consumer loop
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            log.info("Subscribed to {}", INPUT_TOPIC);

            List<Point> batchBuffer = new ArrayList<>();
            Instant lastFlushTime = Instant.now();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Point p = convertToPoint(record.value());
                        if (p != null) {
                            batchBuffer.add(p);
                        }
                    } catch (Exception e) {

                        log.error("Failed to parse record at offset {}: {}", record.offset(), e.getMessage());
                    }
                }

                // Check flush conditions
                boolean isBatchFull = batchBuffer.size() >= BATCH_SIZE;
                boolean isTimeUp = Duration.between(lastFlushTime, Instant.now()).compareTo(FLUSH_INTERVAL) >= 0;

                if (!batchBuffer.isEmpty() && (isBatchFull || isTimeUp)) {
                    flushBatch(writeApi, batchBuffer);
                    // commit offsets after successful DB write
                    consumer.commitSync();
                    batchBuffer.clear();
                    lastFlushTime = Instant.now();
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer waking up for shutdown");
        } catch (Exception e) {
            log.error("Unexpected error in consumer loop", e);
        } finally {
            consumer.close();
            influxClient.close();
            log.info("Sink Stopped");
        }
    }

    private static void flushBatch(WriteApiBlocking writeApi, List<Point> points) {
        try {
            writeApi.writePoints(points);
            log.debug("Flushed {} metrics to InfluxDB", points.size());
        } catch (InfluxException e) {
            log.error("InfluxDB write failed: {}", e.getMessage());
            throw e;
        }
    }

    private static Point convertToPoint(String jsonString) {
        try {
            JsonNode json = mapper.readTree(jsonString);
            return Point.measurement("server_metrics")
                    .time(Instant.now(), WritePrecision.MS)
                    .addTag("host", json.path("host").asText())
                    .addTag("status", json.path("status").asText())
                    .addField("avg", json.path("avg").asDouble())
                    .addField("min", json.path("min").asDouble())
                    .addField("max", json.path("max").asDouble())
                    .addField("count", json.path("count").asLong());
        } catch (Exception e) {
            throw new RuntimeException("Invalid JSON format", e);
        }
    }
}