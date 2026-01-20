package com.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricAggregation {

    @JsonProperty("sum")
    private double sum;

    @JsonProperty("count")
    private long count;

    @JsonProperty("min")
    private double min = Double.MAX_VALUE;

    @JsonProperty("max")
    private double max = Double.MIN_VALUE;

    @JsonProperty("host")
    private String host;

    @JsonProperty("metric_name")
    private String metricName;

    public MetricAggregation() {
    }

    public MetricAggregation add(Metric metric) {
        if (metric == null || metric.getValue() == null) {
            return this;
        }

        double value = metric.getValue();
        this.sum += value;
        this.count++;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);

        // capture metadata from the first metric in the window
        if (this.host == null) {
            this.host = metric.getHost();
            this.metricName = metric.getMetricName();
        }

        return this;
    }

    @JsonIgnore
    public double getAverage() {
        return count > 0 ? sum / count : 0.0;
    }

    @JsonIgnore
    public boolean isBreaching(double threshold) {
        return getAverage() > threshold;
    }

    @JsonIgnore
    public String getSeverity() {
        double avg = getAverage();
        // for demo purposes
        if (avg > 90.0) return "CRITICAL";
        if (avg > 75.0) return "WARNING";
        return "OK";
    }

    // getters for JSON serialization
    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    // prevent sending MAX_VALUE if window was empty
    public double getMin() {
        return count > 0 ? min : 0.0;
    }

    public double getMax() {
        return count > 0 ? max : 0.0;
    }

    public String getHost() {
        return host;
    }

    public String getMetricName() {
        return metricName;
    }
}