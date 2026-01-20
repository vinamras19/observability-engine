package com.engine.producer;

import com.engine.model.Metric;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricProducer {

    private static final Logger log = LoggerFactory.getLogger(MetricProducer.class);

    private static final String TOPIC = getEnv("KAFKA_INPUT_TOPIC", "raw-metrics");
    private static final String BOOTSTRAP_SERVERS = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final int INTERVAL_MS = Integer.parseInt(getEnv("SEND_INTERVAL_MS", "500"));

    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        log.info("Starting Metric Producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        String[] hosts = {"server-alpha", "server-beta", "server-gamma"};

        Map<String, Double> currentCpuLoad = new HashMap<>();
        currentCpuLoad.put("server-alpha", 45.0);
        currentCpuLoad.put("server-beta", 60.0);
        currentCpuLoad.put("server-gamma", 30.0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down producer...");
            running.set(false);
        }));

        log.info("Producer started. Target topic: {}", TOPIC);

        try {
            while (running.get()) {
                for (String host : hosts) {
                    try {
                        // Random Walk Logic
                        // New Value = Old Value + (Random drift between -5% and +5%)
                        double current = currentCpuLoad.get(host);
                        double drift = (random.nextDouble() * 10) - 5;
                        double newValue = Math.min(100, Math.max(0, current + drift));

                        currentCpuLoad.put(host, newValue);

                        Metric metric = new Metric(host, "cpu_usage", newValue);
                        String json = mapper.writeValueAsString(metric);

                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, host, json);

                        producer.send(record, (metadata, ex) -> {
                            if (ex != null) {
                                log.error("Failed to send metric for host {}", host, ex);
                            }
                        });
                    } catch (Exception e) {
                        log.error("Error creating metric for host {}", host, e);
                    }
                }

                try {
                    Thread.sleep(INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            log.info("Flushing and closing producer...");
            producer.flush();
            producer.close();
        }
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}