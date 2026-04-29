package com.engine.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AdvancedSignalAnalyzerTest {

    @Test
    void testKalmanConvergence() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        AdvancedSignalResult result = null;
        for (int i = 0; i < 50; i++) {
            result = analyzer.analyze("host-1", 50.0);
        }

        assertEquals(50.0, result.getKalmanEstimate(), 0.5, "Kalman should converge to constant signal");
        assertTrue(Math.abs(result.getKalmanResidual()) < 1.0, "Residual should be near zero for constant signal");
        assertFalse(result.isKalmanAnomaly(), "Stable signal should not trigger Kalman anomaly");
    }

    @Test
    void testKalmanAnomalyDetection() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        // baseline at 50
        for (int i = 0; i < 30; i++) {
            analyzer.analyze("host-1", 50.0);
        }

        // sudden spike to 90
        AdvancedSignalResult result = analyzer.analyze("host-1", 90.0);
        assertTrue(result.isKalmanAnomaly(), "Sudden spike should trigger Kalman anomaly");
        assertTrue(result.getKalmanResidual() > 30.0, "Residual should reflect the spike magnitude");
    }

    @Test
    void testKalmanTracksGradualDrift() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        // slowly drift from 50 to 70
        AdvancedSignalResult result = null;
        for (int i = 0; i < 100; i++) {
            double value = 50.0 + (i * 0.2);
            result = analyzer.analyze("host-1", value);
        }

        // Kalman should have tracked the drift
        assertFalse(result.isKalmanAnomaly(), "Gradual drift should not trigger anomaly");
        assertTrue(result.getKalmanEstimate() > 60.0, "Kalman estimate should follow the drift");
    }

    @Test
    void testKalmanStableWithNoise() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        boolean anyAnomaly = false;
        for (int i = 0; i < 100; i++) {
            double value = 50.0 + (Math.random() - 0.5) * 4;
            AdvancedSignalResult result = analyzer.analyze("host-1", value);
            if (result.isKalmanAnomaly()) {
                anyAnomaly = true;
            }
        }

        assertFalse(anyAnomaly, "Minor noise should not trigger Kalman anomaly");
    }

    @Test
    void testBayesianDetectsRegimeChange() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer(
                1.0, 500.0, 3.0, 2.0, 0.3, 200);

        // stable regime at 50
        for (int i = 0; i < 60; i++) {
            analyzer.analyze("host-1", 50.0 + (i % 2 == 0 ? 0.5 : -0.5));
        }

        // abrupt shift to 80
        boolean detected = false;
        for (int i = 0; i < 40; i++) {
            AdvancedSignalResult result = analyzer.analyze("host-1", 80.0 + (i % 2 == 0 ? 0.5 : -0.5));
            if (result.isBayesianAlert()) {
                detected = true;
                break;
            }
        }

        assertTrue(detected, "Bayesian CPD should detect regime shift from 50 to 80");
    }

    @Test
    void testBayesianStableSignal() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        int alertCount = 0;
        // skip first few as detector warms up
        for (int i = 0; i < 100; i++) {
            double value = 50.0 + (Math.random() - 0.5) * 2;
            AdvancedSignalResult result = analyzer.analyze("host-1", value);
            if (i > 20 && result.isBayesianAlert()) {
                alertCount++;
            }
        }

        assertTrue(alertCount < 5, "Stable signal should produce very few Bayesian alerts");
    }

    @Test
    void testHostIsolation() {
        AdvancedSignalAnalyzer analyzer = new AdvancedSignalAnalyzer();

        for (int i = 0; i < 30; i++) {
            analyzer.analyze("host-a", 90.0);
            analyzer.analyze("host-b", 10.0);
        }

        AdvancedSignalResult a = analyzer.analyze("host-a", 90.0);
        AdvancedSignalResult b = analyzer.analyze("host-b", 10.0);

        assertTrue(a.getKalmanEstimate() > 85.0, "Host A Kalman should track near 90");
        assertTrue(b.getKalmanEstimate() < 15.0, "Host B Kalman should track near 10");
    }
}