package com.engine.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AdvancedSignalResult {

    @JsonProperty("host")
    private final String host;

    @JsonProperty("raw_value")
    private final double rawValue;

    @JsonProperty("kalman_predicted")
    private final double kalmanPredicted;

    @JsonProperty("kalman_residual")
    private final double kalmanResidual;

    @JsonProperty("residual_variance")
    private final double residualVariance;

    @JsonProperty("kalman_estimate")
    private final double kalmanEstimate;

    @JsonProperty("kalman_anomaly")
    private final boolean kalmanAnomaly;

    @JsonProperty("change_point_prob")
    private final double changePointProb;

    @JsonProperty("bayesian_alert")
    private final boolean bayesianAlert;

    public AdvancedSignalResult(String host, double rawValue,
                                double kalmanPredicted, double kalmanResidual,
                                double residualVariance, double kalmanEstimate,
                                boolean kalmanAnomaly,
                                double changePointProb, boolean bayesianAlert) {
        this.host = host;
        this.rawValue = rawValue;
        this.kalmanPredicted = kalmanPredicted;
        this.kalmanResidual = kalmanResidual;
        this.residualVariance = residualVariance;
        this.kalmanEstimate = kalmanEstimate;
        this.kalmanAnomaly = kalmanAnomaly;
        this.changePointProb = changePointProb;
        this.bayesianAlert = bayesianAlert;
    }

    public String getHost()              { return host; }
    public double getRawValue()          { return rawValue; }
    public double getKalmanPredicted()   { return kalmanPredicted; }
    public double getKalmanResidual()    { return kalmanResidual; }
    public double getResidualVariance()  { return residualVariance; }
    public double getKalmanEstimate()    { return kalmanEstimate; }
    public boolean isKalmanAnomaly()     { return kalmanAnomaly; }
    public double getChangePointProb()   { return changePointProb; }
    public boolean isBayesianAlert()     { return bayesianAlert; }
}