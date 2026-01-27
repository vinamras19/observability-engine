package com.engine.core;

import com.engine.model.Metric;
import com.engine.model.MetricAggregation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class AnalyticsEngine {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsEngine.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // Configuration
    private static final String BOOTSTRAP_SERVERS = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String APPLICATION_ID = getEnv("KAFKA_APPLICATION_ID", "observability-engine");
    private static final String INPUT_TOPIC = getEnv("KAFKA_INPUT_TOPIC", "raw-metrics");
    private static final String OUTPUT_TOPIC = getEnv("KAFKA_OUTPUT_TOPIC", "analyzed-metrics");
    private static final String ALERTS_TOPIC = getEnv("KAFKA_ALERTS_TOPIC", "metric-alerts");
    private static final double ALERT_THRESHOLD = Double.parseDouble(getEnv("ALERT_THRESHOLD", "85.0"));
    private static final int WINDOW_SECONDS = Integer.parseInt(getEnv("WINDOW_SECONDS", "60"));

    public static void main(String[] args) {
        log.info("Starting Analytics Engine...");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        Topology topology = buildTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);

        //shutdown handler
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down streams application...");
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        try {
            streams.start();
            log.info("Engine started successfully");
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Fatal error in streams application", e);
            System.exit(1);
        }
    }

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawStream = builder.stream(INPUT_TOPIC);

        //Parse and Validate
        //Used flatMapValues for exceptions (Poison Pill pattern)
        KStream<String, Metric> metricStream = rawStream
                .flatMapValues(json -> {
                    try {
                        Metric metric = mapper.readValue(json, Metric.class);
                        if (metric != null && metric.isValid()) {
                            return Collections.singletonList(metric);
                        }
                    } catch (Exception e) {
                        // Log only the error message to avoid flooding logs with stack traces on bad data
                        log.warn("Skipping malformed JSON: {} (Error: {})", json, e.getMessage());
                    }
                    return Collections.emptyList();
                });

        //Aggregate by Key (Host) over Time Windows
        KTable<Windowed<String>, MetricAggregation> aggregated = metricStream
                .groupByKey(Grouped.with(Serdes.String(), CustomSerdes.jsonSerde(Metric.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(WINDOW_SECONDS), Duration.ofSeconds(10)))
                .aggregate(
                        MetricAggregation::new,
                        (key, metric, agg) -> agg.add(metric),
                        Materialized.<String, MetricAggregation, WindowStore<Bytes, byte[]>>as("metrics-store")
                                .withValueSerde(CustomSerdes.jsonSerde(MetricAggregation.class))
                );

        //Analysis Results
        aggregated.toStream()
                .map((window, agg) -> {
                    String output = String.format(
                            "{\"host\":\"%s\",\"metric\":\"%s\",\"avg\":%.2f,\"min\":%.2f,\"max\":%.2f,\"count\":%d,\"status\":\"%s\"}",
                            window.key(), agg.getMetricName(), agg.getAverage(),
                            agg.getMin(), agg.getMax(), agg.getCount(),
                            agg.isBreaching(ALERT_THRESHOLD) ? "BREACH" : "NORMAL"
                    );
                    return new KeyValue<>(window.key(), output);
                })
                .to(OUTPUT_TOPIC);

        //Alerting Logic
        aggregated.toStream()
                .filter((window, agg) -> agg != null && agg.isBreaching(ALERT_THRESHOLD))
                .map((window, agg) -> {
                    String alert = String.format(
                            "{\"type\":\"THRESHOLD_BREACH\",\"host\":\"%s\",\"metric\":\"%s\",\"avg\":%.2f,\"threshold\":%.2f,\"severity\":\"%s\"}",
                            window.key(), agg.getMetricName(), agg.getAverage(), ALERT_THRESHOLD, agg.getSeverity()
                    );
                    log.warn("ALARM: Host {} breached threshold. Avg CPU: {}%", window.key(), String.format("%.2f", agg.getAverage()));
                    return new KeyValue<>(window.key(), alert);
                })
                .to(ALERTS_TOPIC);

        return builder.build();
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}