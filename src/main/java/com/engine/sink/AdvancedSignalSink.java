package com.engine.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
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

public class AdvancedSignalSink {

    private static final Logger log = LoggerFactory.getLogger(AdvancedSignalSink.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // Config
    private static final String INFLUX_URL = System.getenv("INFLUXDB_URL") != null ? System.getenv("INFLUXDB_URL") : "http://localhost:8086";
    private static final String INFLUX_TOKEN = System.getenv("INFLUXDB_TOKEN") != null ? System.getenv("INFLUXDB_TOKEN") : "my-token";
    private static final String INFLUX_ORG = "engine-org";
    private static final String INFLUX_BUCKET = "telemetry";
    private static final String BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") : "localhost:9092";
    private static final String INPUT_TOPIC = "advanced-signals";

    // Batching Config
    private static final int BATCH_SIZE = 100;
    private static final Duration FLUSH_INTERVAL = Duration.ofSeconds(3);

    public static void main(String[] args) {
        log.info("Starting Advanced Signal Sink...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "advanced-signal-sink-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        InfluxDBClient influxClient = InfluxDBClientFactory.create(INFLUX_URL, INFLUX_TOKEN.toCharArray(), INFLUX_ORG, INFLUX_BUCKET);
        WriteApiBlocking writeApi = influxClient.getWriteApiBlocking();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            consumer.wakeup();
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
                        log.error("Failed to parse advanced signal record at offset {}: {}", record.offset(), e.getMessage());
                    }
                }

                boolean isBatchFull = batchBuffer.size() >= BATCH_SIZE;
                boolean isTimeUp = Duration.between(lastFlushTime, Instant.now()).compareTo(FLUSH_INTERVAL) >= 0;

                if (!batchBuffer.isEmpty() && (isBatchFull || isTimeUp)) {
                    writeApi.writePoints(batchBuffer);
                    consumer.commitSync();
                    log.debug("Flushed {} advanced signal points to InfluxDB", batchBuffer.size());
                    batchBuffer.clear();
                    lastFlushTime = Instant.now();
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer waking up for shutdown");
        } catch (Exception e) {
            log.error("Unexpected error in advanced signal sink", e);
        } finally {
            consumer.close();
            influxClient.close();
            log.info("Advanced Signal Sink stopped");
        }
    }

    private static Point convertToPoint(String jsonString) {
        try {
            JsonNode json = mapper.readTree(jsonString);
            return Point.measurement("advanced_signal_analysis")
                    .time(Instant.now(), WritePrecision.MS)
                    .addTag("host", json.path("host").asText())
                    .addTag("kalman_anomaly", String.valueOf(json.path("kalman_anomaly").asBoolean()))
                    .addTag("bayesian_alert", String.valueOf(json.path("bayesian_alert").asBoolean()))
                    .addField("raw_avg", json.path("raw_avg").asDouble())
                    .addField("kalman_predicted", json.path("kalman_predicted").asDouble())
                    .addField("kalman_residual", json.path("kalman_residual").asDouble())
                    .addField("residual_variance", json.path("residual_variance").asDouble())
                    .addField("kalman_estimate", json.path("kalman_estimate").asDouble())
                    .addField("change_point_prob", json.path("change_point_prob").asDouble());
        } catch (Exception e) {
            throw new RuntimeException("Invalid advanced signal JSON", e);
        }
    }
}