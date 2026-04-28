package com.engine.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdvancedSignalAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(AdvancedSignalAnalyzer.class);

    // Kalman Filter
    private final double processNoise;
    private final double measurementNoise;
    private final double kalmanThreshold;

    // Bayesian Change-Point Detection
    private final double hazardRate;
    private final double bayesianThreshold;
    private final int maxRunLength;

    private final Map<String, HostState> states = new ConcurrentHashMap<>();

    private static class HostState {
        // Kalman — 1D random walk model
        double kalmanEstimate;
        double kalmanCovariance;
        boolean kalmanInitialized;

        // Bayesian CPD — run length distribution + per-run sufficient stats
        double[] runLengthProbs;
        double[] suffMean;
        double[] suffM2;
        int[] suffCount;
        int maxRL;

        HostState(int maxRunLength) {
            this.maxRL = maxRunLength;
            this.runLengthProbs = new double[maxRunLength];
            this.suffMean = new double[maxRunLength];
            this.suffM2 = new double[maxRunLength];
            this.suffCount = new int[maxRunLength];
            this.runLengthProbs[0] = 1.0;
        }
    }

    public AdvancedSignalAnalyzer(double processNoise, double measurementNoise, double kalmanThreshold,
                                  double hazardRate, double bayesianThreshold, int maxRunLength) {
        this.processNoise = processNoise;
        this.measurementNoise = measurementNoise;
        this.kalmanThreshold = kalmanThreshold;
        this.hazardRate = hazardRate;
        this.bayesianThreshold = bayesianThreshold;
        this.maxRunLength = maxRunLength;
    }

    // defaults tuned for 60s CPU metric windows
    public AdvancedSignalAnalyzer() {
        this(1.0, 5.0, 3.0, 50.0, 0.5, 200);
    }

    public AdvancedSignalResult analyze(String host, double value) {
        HostState state = states.computeIfAbsent(host, k -> new HostState(maxRunLength));

        // Kalman Filter
        double predicted;
        double residual;
        double residualVar;
        boolean kalmanAnomaly;

        if (!state.kalmanInitialized) {
            state.kalmanEstimate = value;
            state.kalmanCovariance = 1.0;
            state.kalmanInitialized = true;
            predicted = value;
            residual = 0.0;
            residualVar = state.kalmanCovariance + measurementNoise;
            kalmanAnomaly = false;
        } else {
            // predict step (random walk: state doesn't change, covariance grows)
            double predState = state.kalmanEstimate;
            double predCov = state.kalmanCovariance + processNoise;

            predicted = predState;
            residual = value - predState;
            residualVar = predCov + measurementNoise;

            double normResidual = residual / Math.sqrt(residualVar);
            kalmanAnomaly = Math.abs(normResidual) > kalmanThreshold;

            // update step
            double gain = predCov / residualVar;
            state.kalmanEstimate = predState + gain * residual;
            state.kalmanCovariance = (1.0 - gain) * predCov;
        }

        // Bayesian Change-Point Detection
        double cpProb = computeChangePoint(state, value);
        boolean bayesianAlert = cpProb > bayesianThreshold;

        if (kalmanAnomaly) {
            log.info("KALMAN: anomaly on host {} (residual={}, predicted={})",
                    host, String.format("%.2f", residual), String.format("%.2f", predicted));
        }
        if (bayesianAlert) {
            log.info("BAYESIAN_CPD: change point on host {} (prob={})",
                    host, String.format("%.4f", cpProb));
        }

        return new AdvancedSignalResult(
                host, value, predicted, residual, residualVar,
                state.kalmanEstimate, kalmanAnomaly,
                cpProb, bayesianAlert
        );
    }

    private double computeChangePoint(HostState state, double value) {
        double[] newProbs = new double[state.maxRL];
        double hazard = 1.0 / hazardRate;
        double changeMass = 0.0;

        for (int r = state.maxRL - 2; r >= 0; r--) {
            if (state.runLengthProbs[r] < 1e-10) continue;

            // predictive probability for this run length
            double predMean;
            double predVar;
            if (state.suffCount[r] == 0) {
                predMean = 50.0;
                predVar = 1000.0;
            } else {
                predMean = state.suffMean[r];
                double sampleVar = state.suffCount[r] > 1
                        ? state.suffM2[r] / (state.suffCount[r] - 1)
                        : measurementNoise;
                predVar = sampleVar + measurementNoise;
            }
            predVar = Math.max(predVar, 0.01);

            double prob = gaussianPdf(value, predMean, predVar);
            prob = Math.max(prob, 1e-8);  // to prevent total probability collapse

            // growth: run continues
            newProbs[r + 1] += state.runLengthProbs[r] * (1.0 - hazard) * prob;

            // change: run resets
            changeMass += state.runLengthProbs[r] * hazard * prob;
        }

        newProbs[0] = changeMass;

        // normalize
        double total = 0.0;
        for (double p : newProbs) total += p;
        if (total > 0) {
            for (int i = 0; i < state.maxRL; i++) newProbs[i] /= total;
        }

        // update sufficient statistics (Welford's)
        double[] newMean = new double[state.maxRL];
        double[] newM2 = new double[state.maxRL];
        int[] newCount = new int[state.maxRL];

        // run length 0 starts fresh
        newMean[0] = value;
        newM2[0] = 0.0;
        newCount[0] = 1;

        for (int r = 1; r < state.maxRL; r++) {
            if (newProbs[r] < 1e-10) continue;
            int prevR = r - 1;
            int n = state.suffCount[prevR] + 1;
            double oldMean = state.suffMean[prevR];
            double delta = value - oldMean;
            newMean[r] = oldMean + delta / n;
            newM2[r] = state.suffM2[prevR] + delta * (value - newMean[r]);
            newCount[r] = n;
        }

        state.runLengthProbs = newProbs;
        state.suffMean = newMean;
        state.suffM2 = newM2;
        state.suffCount = newCount;

        return newProbs[0];
    }

    private double gaussianPdf(double x, double mean, double variance) {
        double diff = x - mean;
        return Math.exp(-0.5 * diff * diff / variance) / Math.sqrt(2.0 * Math.PI * variance);
    }
}