package com.engine.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SignalAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(SignalAnalyzer.class);

    // EWMA
    private final double alpha;

    // CUSUM
    private final double cusumK;
    private final double cusumH;

    // Entropy
    private final int entropyWindowSize;
    private final int entropyBins;

    private final Map<String, HostState> states = new ConcurrentHashMap<>();

    private static class HostState {
        double ewma;
        boolean initialized;

        // CUSUM — Welford's online mean + variance
        double cusumHigh;
        double cusumLow;
        double runningMean;
        double runningM2;
        long sampleCount;

        // sliding window for entropy
        double[] window;
        int windowIndex;
        int windowFilled;

        HostState(int windowSize) {
            this.window = new double[windowSize];
        }
    }

    public SignalAnalyzer(double alpha, double cusumK, double cusumH,
                          int entropyWindowSize, int entropyBins) {
        this.alpha = alpha;
        this.cusumK = cusumK;
        this.cusumH = cusumH;
        this.entropyWindowSize = entropyWindowSize;
        this.entropyBins = entropyBins;
    }

    // defaults tuned for 60s CPU metric windows
    public SignalAnalyzer() {
        this(0.3, 0.5, 4.0, 30, 20);
    }

    public SignalResult analyze(String host, double value) {
        HostState state = states.computeIfAbsent(host, k -> new HostState(entropyWindowSize));

        // EWMA
        if (!state.initialized) {
            state.ewma = value;
            state.initialized = true;
        } else {
            state.ewma = alpha * value + (1.0 - alpha) * state.ewma;
        }

        // CUSUM — update running stats
        state.sampleCount++;
        double oldMean = state.runningMean;
        state.runningMean += (value - state.runningMean) / state.sampleCount;
        state.runningM2 += (value - oldMean) * (value - state.runningMean);

        double sigma = state.sampleCount > 1
                ? Math.sqrt(state.runningM2 / (state.sampleCount - 1))
                : 1.0;

        double z = (value - state.runningMean) / Math.max(sigma, 0.001);

        state.cusumHigh = Math.max(0, state.cusumHigh + z - cusumK);
        state.cusumLow = Math.max(0, state.cusumLow - z - cusumK);

        boolean cusumAlert = state.cusumHigh > cusumH || state.cusumLow > cusumH;
        String shiftDirection = "NONE";
        if (state.cusumHigh > cusumH) {
            shiftDirection = "UPWARD_SHIFT";
            state.cusumHigh = 0;
        } else if (state.cusumLow > cusumH) {
            shiftDirection = "DOWNWARD_SHIFT";
            state.cusumLow = 0;
        }

        // Entropy — sliding window histogram
        state.window[state.windowIndex] = value;
        state.windowIndex = (state.windowIndex + 1) % entropyWindowSize;
        if (state.windowFilled < entropyWindowSize) {
            state.windowFilled++;
        }

        double entropy = computeEntropy(state.window, state.windowFilled);
        double maxEntropy = Math.log(Math.min(state.windowFilled, entropyBins)) / Math.log(2);
        double normalizedEntropy = maxEntropy > 0 ? entropy / maxEntropy : 0;

        if (cusumAlert) {
            log.info("CUSUM: {} on host {} (sigma={}, mean={})",
                    shiftDirection, host,
                    String.format("%.2f", sigma),
                    String.format("%.2f", state.runningMean));
        }

        return new SignalResult(
                host, value, state.ewma,
                state.cusumHigh, state.cusumLow, cusumAlert, shiftDirection,
                entropy, normalizedEntropy, sigma, state.runningMean
        );
    }

    private double computeEntropy(double[] values, int count) {
        if (count == 0) return 0;

        int[] histogram = new int[entropyBins];
        for (int i = 0; i < count; i++) {
            int bin = (int) (values[i] * entropyBins / 100.0);
            bin = Math.max(0, Math.min(entropyBins - 1, bin));
            histogram[bin]++;
        }

        double entropy = 0.0;
        for (int freq : histogram) {
            if (freq > 0) {
                double p = (double) freq / count;
                entropy -= p * (Math.log(p) / Math.log(2));
            }
        }
        return entropy;
    }
}