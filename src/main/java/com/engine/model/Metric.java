package com.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Metric {

    @JsonProperty("host")
    public String host;

    @JsonProperty("metric_name")
    public String metricName;

    @JsonProperty("value")
    public Double value;

    @JsonProperty("timestamp")
    public Long timestamp;

    // No-args constructor for Jackson deserialization
    public Metric() {
    }

    public Metric(String host, String metricName, Double value) {
        this.host = host;
        this.metricName = metricName;
        this.value = value;

        this.timestamp = System.currentTimeMillis();
    }

    public boolean isValid() {
        // sanity checks
        return host != null && !host.isBlank()
                && metricName != null && !metricName.isBlank()
                && value != null
                && !Double.isNaN(value) && !Double.isInfinite(value);
    }

    public String getHost() {
        return host;
    }

    public String getMetricName() {
        return metricName;
    }

    public Double getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s: %.2f", host, metricName, value);
    }
}