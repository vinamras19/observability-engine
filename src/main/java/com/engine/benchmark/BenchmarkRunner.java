package com.engine.benchmark;

import com.engine.core.SignalAnalyzer;
import com.engine.core.SignalResult;
import com.engine.core.AdvancedSignalAnalyzer;
import com.engine.core.AdvancedSignalResult;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class BenchmarkRunner {

    private static final int NUM_TRIALS = 1000;
    private static final int BASELINE_LENGTH = 100;
    private static final int POST_CHANGE_LENGTH = 100;
    private static final double BASE_MEAN = 50.0;

    private static final double[] SHIFT_SIZES = {1.0, 2.0, 5.0};
    private static final double[] NOISE_LEVELS = {2.0, 5.0, 10.0};

    private static final Random rng = new Random(42);

    // detector parameter profiles
    static class DetectorConfig {
        final String label;
        // CUSUM
        final double cusumAlpha, cusumK, cusumH;
        // Kalman
        final double kalmanProcessNoise, kalmanMeasurementNoise, kalmanThreshold;
        // Bayesian
        final double bayesianHazardRate, bayesianThreshold;
        final int maxRunLength;

        DetectorConfig(String label,
                       double cusumAlpha, double cusumK, double cusumH,
                       double kalmanProcessNoise, double kalmanMeasurementNoise, double kalmanThreshold,
                       double bayesianHazardRate, double bayesianThreshold, int maxRunLength) {
            this.label = label;
            this.cusumAlpha = cusumAlpha;
            this.cusumK = cusumK;
            this.cusumH = cusumH;
            this.kalmanProcessNoise = kalmanProcessNoise;
            this.kalmanMeasurementNoise = kalmanMeasurementNoise;
            this.kalmanThreshold = kalmanThreshold;
            this.bayesianHazardRate = bayesianHazardRate;
            this.bayesianThreshold = bayesianThreshold;
            this.maxRunLength = maxRunLength;
        }
    }

    // default: production parameters from constructors
    private static final DetectorConfig DEFAULTS = new DetectorConfig(
            "default",
            0.3, 0.5, 4.0,     // CUSUM
            1.0, 5.0, 3.0,     // Kalman
            50.0, 0.5, 200      // Bayesian
    );

    // tuned: calibrated for benchmark signal characteristics
    private static final DetectorConfig TUNED = new DetectorConfig(
            "tuned",
            0.3, 1.0, 8.0,      // CUSUM — higher k filters more noise, higher h reduces FPR
            1.0, 50.0, 3.0,     // Kalman — measurement noise matched to higher sigma range
            20.0, 0.3, 200       // Bayesian — lower hazard rate = more sensitive, lower threshold
    );

    public static void main(String[] args) throws IOException {
        System.out.println("Detection Benchmark: CUSUM vs Kalman vs Bayesian CPD\n");

        List<BenchmarkResult> allResults = new ArrayList<>();

        // Round 1: default parameters
        System.out.println("=== ROUND 1: DEFAULT PARAMETERS ===\n");
        runRound(DEFAULTS, allResults);

        // Round 2: tuned parameters
        System.out.println("=== ROUND 2: TUNED PARAMETERS ===\n");
        runRound(TUNED, allResults);

        writeCSV(allResults, "docs/benchmark-results.csv");
        System.out.println("\nResults saved to docs/benchmark-results.csv");
    }

    private static void runRound(DetectorConfig config, List<BenchmarkResult> allResults) {
        for (double noise : NOISE_LEVELS) {
            for (double shiftSigma : SHIFT_SIZES) {
                double shiftSize = shiftSigma * noise;
                System.out.printf("Noise=%.1f, Shift=%.1f sigma (%.1f absolute)%n",
                        noise, shiftSigma, shiftSize);

                DetectionStats cusumStats = runDetectionExperiment("CUSUM", config, noise, shiftSize);
                DetectionStats kalmanStats = runDetectionExperiment("KALMAN", config, noise, shiftSize);
                DetectionStats bayesianStats = runDetectionExperiment("BAYESIAN", config, noise, shiftSize);

                double cusumFPR = runFalsePositiveExperiment("CUSUM", config, noise);
                double kalmanFPR = runFalsePositiveExperiment("KALMAN", config, noise);
                double bayesianFPR = runFalsePositiveExperiment("BAYESIAN", config, noise);

                allResults.add(new BenchmarkResult(config.label, noise, shiftSigma, "CUSUM",
                        cusumStats.meanDelay, cusumStats.medianDelay, cusumStats.detectionRate, cusumFPR));
                allResults.add(new BenchmarkResult(config.label, noise, shiftSigma, "KALMAN",
                        kalmanStats.meanDelay, kalmanStats.medianDelay, kalmanStats.detectionRate, kalmanFPR));
                allResults.add(new BenchmarkResult(config.label, noise, shiftSigma, "BAYESIAN",
                        bayesianStats.meanDelay, bayesianStats.medianDelay, bayesianStats.detectionRate, bayesianFPR));

                System.out.printf("  CUSUM    -> delay: %.1f (med: %.0f), detection: %.1f%%, FPR: %.2f%%%n",
                        cusumStats.meanDelay, cusumStats.medianDelay, cusumStats.detectionRate * 100, cusumFPR * 100);
                System.out.printf("  KALMAN   -> delay: %.1f (med: %.0f), detection: %.1f%%, FPR: %.2f%%%n",
                        kalmanStats.meanDelay, kalmanStats.medianDelay, kalmanStats.detectionRate * 100, kalmanFPR * 100);
                System.out.printf("  BAYESIAN -> delay: %.1f (med: %.0f), detection: %.1f%%, FPR: %.2f%%%n",
                        bayesianStats.meanDelay, bayesianStats.medianDelay, bayesianStats.detectionRate * 100, bayesianFPR * 100);
                System.out.println();
            }
        }
    }

    private static DetectionStats runDetectionExperiment(String detector, DetectorConfig config,
                                                         double noise, double shiftSize) {
        List<Integer> delays = new ArrayList<>();
        int detected = 0;

        for (int trial = 0; trial < NUM_TRIALS; trial++) {
            String host = "bench-" + trial;
            int delay = runSingleDetectionTrial(detector, config, host, noise, shiftSize);
            if (delay >= 0) {
                delays.add(delay);
                detected++;
            }
        }

        double meanDelay = delays.stream().mapToInt(Integer::intValue).average().orElse(-1);
        double medianDelay = delays.isEmpty() ? -1 : median(delays);
        double detectionRate = (double) detected / NUM_TRIALS;

        return new DetectionStats(meanDelay, medianDelay, detectionRate);
    }

    private static int runSingleDetectionTrial(String detector, DetectorConfig config,
                                               String host, double noise, double shiftSize) {
        SignalAnalyzer cusum = null;
        AdvancedSignalAnalyzer advanced = null;

        if ("CUSUM".equals(detector)) {
            cusum = new SignalAnalyzer(config.cusumAlpha, config.cusumK, config.cusumH, 30, 20);
        } else {
            advanced = new AdvancedSignalAnalyzer(
                    config.kalmanProcessNoise, config.kalmanMeasurementNoise, config.kalmanThreshold,
                    config.bayesianHazardRate, config.bayesianThreshold, config.maxRunLength);
        }

        // baseline
        for (int i = 0; i < BASELINE_LENGTH; i++) {
            double value = BASE_MEAN + rng.nextGaussian() * noise;
            if ("CUSUM".equals(detector)) {
                cusum.analyze(host, value);
            } else {
                advanced.analyze(host, value);
            }
        }

        // post-change
        double newMean = BASE_MEAN + shiftSize;
        for (int i = 0; i < POST_CHANGE_LENGTH; i++) {
            double value = newMean + rng.nextGaussian() * noise;

            if ("CUSUM".equals(detector)) {
                SignalResult result = cusum.analyze(host, value);
                if (result.isCusumAlert()) {
                    return i + 1;
                }
            } else if ("KALMAN".equals(detector)) {
                AdvancedSignalResult result = advanced.analyze(host, value);
                if (result.isKalmanAnomaly()) {
                    return i + 1;
                }
            } else {
                AdvancedSignalResult result = advanced.analyze(host, value);
                if (result.isBayesianAlert()) {
                    return i + 1;
                }
            }
        }

        return -1;
    }

    private static double runFalsePositiveExperiment(String detector, DetectorConfig config, double noise) {
        int falsePositives = 0;

        for (int trial = 0; trial < NUM_TRIALS; trial++) {
            String host = "fp-" + trial;
            boolean fired = runSingleFalsePositiveTrial(detector, config, host, noise);
            if (fired) falsePositives++;
        }

        return (double) falsePositives / NUM_TRIALS;
    }

    private static boolean runSingleFalsePositiveTrial(String detector, DetectorConfig config,
                                                       String host, double noise) {
        SignalAnalyzer cusum = null;
        AdvancedSignalAnalyzer advanced = null;

        if ("CUSUM".equals(detector)) {
            cusum = new SignalAnalyzer(config.cusumAlpha, config.cusumK, config.cusumH, 30, 20);
        } else {
            advanced = new AdvancedSignalAnalyzer(
                    config.kalmanProcessNoise, config.kalmanMeasurementNoise, config.kalmanThreshold,
                    config.bayesianHazardRate, config.bayesianThreshold, config.maxRunLength);
        }

        int totalSamples = BASELINE_LENGTH + POST_CHANGE_LENGTH;

        for (int i = 0; i < totalSamples; i++) {
            double value = BASE_MEAN + rng.nextGaussian() * noise;

            if ("CUSUM".equals(detector)) {
                SignalResult result = cusum.analyze(host, value);
                if (result.isCusumAlert()) return true;
            } else if ("KALMAN".equals(detector)) {
                AdvancedSignalResult result = advanced.analyze(host, value);
                if (result.isKalmanAnomaly()) return true;
            } else {
                AdvancedSignalResult result = advanced.analyze(host, value);
                if (result.isBayesianAlert()) return true;
            }
        }

        return false;
    }

    private static double median(List<Integer> values) {
        List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        if (n % 2 == 0) {
            return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
        }
        return sorted.get(n / 2);
    }

    private static void writeCSV(List<BenchmarkResult> results, String path) throws IOException {
        java.io.File file = new java.io.File(path);
        file.getParentFile().mkdirs();

        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.println("config,noise,shift_sigma,detector,mean_delay,median_delay,detection_rate,false_positive_rate");
            for (BenchmarkResult r : results) {
                pw.printf("%s,%.1f,%.1f,%s,%.2f,%.0f,%.4f,%.4f%n",
                        r.config, r.noise, r.shiftSigma, r.detector,
                        r.meanDelay, r.medianDelay, r.detectionRate, r.falsePositiveRate);
            }
        }
    }

    private static class DetectionStats {
        final double meanDelay;
        final double medianDelay;
        final double detectionRate;

        DetectionStats(double meanDelay, double medianDelay, double detectionRate) {
            this.meanDelay = meanDelay;
            this.medianDelay = medianDelay;
            this.detectionRate = detectionRate;
        }
    }

    private static class BenchmarkResult {
        final String config;
        final double noise;
        final double shiftSigma;
        final String detector;
        final double meanDelay;
        final double medianDelay;
        final double detectionRate;
        final double falsePositiveRate;

        BenchmarkResult(String config, double noise, double shiftSigma, String detector,
                        double meanDelay, double medianDelay,
                        double detectionRate, double falsePositiveRate) {
            this.config = config;
            this.noise = noise;
            this.shiftSigma = shiftSigma;
            this.detector = detector;
            this.meanDelay = meanDelay;
            this.medianDelay = medianDelay;
            this.detectionRate = detectionRate;
            this.falsePositiveRate = falsePositiveRate;
        }
    }
}