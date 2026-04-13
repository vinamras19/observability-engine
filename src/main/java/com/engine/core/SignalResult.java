package com.engine.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SignalResult {

    @JsonProperty("host")
    private final String host;

    @JsonProperty("raw_value")
    private final double rawValue;

    @JsonProperty("ewma")
    private final double ewma;

    @JsonProperty("cusum_high")
    private final double cusumHigh;

    @JsonProperty("cusum_low")
    private final double cusumLow;

    @JsonProperty("cusum_alert")
    private final boolean cusumAlert;

    @JsonProperty("shift_direction")
    private final String shiftDirection;

    @JsonProperty("entropy")
    private final double entropy;

    @JsonProperty("normalized_entropy")
    private final double normalizedEntropy;

    @JsonProperty("sigma")
    private final double sigma;

    @JsonProperty("running_mean")
    private final double runningMean;

    public SignalResult(String host, double rawValue, double ewma,
                        double cusumHigh, double cusumLow,
                        boolean cusumAlert, String shiftDirection,
                        double entropy, double normalizedEntropy,
                        double sigma, double runningMean) {
        this.host = host;
        this.rawValue = rawValue;
        this.ewma = ewma;
        this.cusumHigh = cusumHigh;
        this.cusumLow = cusumLow;
        this.cusumAlert = cusumAlert;
        this.shiftDirection = shiftDirection;
        this.entropy = entropy;
        this.normalizedEntropy = normalizedEntropy;
        this.sigma = sigma;
        this.runningMean = runningMean;
    }

    public String getHost()              { return host; }
    public double getRawValue()          { return rawValue; }
    public double getEwma()              { return ewma; }
    public double getCusumHigh()         { return cusumHigh; }
    public double getCusumLow()          { return cusumLow; }
    public boolean isCusumAlert()        { return cusumAlert; }
    public String getShiftDirection()    { return shiftDirection; }
    public double getEntropy()           { return entropy; }
    public double getNormalizedEntropy() { return normalizedEntropy; }
    public double getSigma()             { return sigma; }
    public double getRunningMean()       { return runningMean; }
}