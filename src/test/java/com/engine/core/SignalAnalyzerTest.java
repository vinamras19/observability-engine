package com.engine.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SignalAnalyzerTest {

    @Test
    void testEwmaConvergence() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 4.0, 30, 20);

        for (int i = 0; i < 50; i++) {
            analyzer.analyze("host-1", 50.0);
        }
        SignalResult result = analyzer.analyze("host-1", 50.0);

        assertEquals(50.0, result.getEwma(), 0.01, "EWMA should converge to constant signal");
        assertFalse(result.isCusumAlert(), "Stable signal should not trigger CUSUM");
    }

    @Test
    void testEwmaSmoothing() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 100.0, 30, 20);

        SignalResult result = null;
        for (int i = 0; i < 100; i++) {
            double value = (i % 2 == 0) ? 40.0 : 60.0;
            result = analyzer.analyze("host-1", value);
        }

        assertTrue(result.getEwma() > 45.0 && result.getEwma() < 55.0,
                "EWMA should smooth oscillation to near 50");
    }

    @Test
    void testCusumUpwardShift() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 4.0, 30, 20);

        // baseline at 50
        for (int i = 0; i < 40; i++) {
            analyzer.analyze("host-1", 50.0 + (Math.random() - 0.5) * 2);
        }

        // sustained jump to 80
        boolean detected = false;
        for (int i = 0; i < 20; i++) {
            SignalResult result = analyzer.analyze("host-1", 80.0);
            if (result.isCusumAlert() && "UPWARD_SHIFT".equals(result.getShiftDirection())) {
                detected = true;
                break;
            }
        }

        assertTrue(detected, "CUSUM should detect upward shift from 50 to 80");
    }

    @Test
    void testCusumDownwardShift() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 4.0, 30, 20);

        for (int i = 0; i < 40; i++) {
            analyzer.analyze("host-1", 70.0 + (Math.random() - 0.5) * 2);
        }

        boolean detected = false;
        for (int i = 0; i < 20; i++) {
            SignalResult result = analyzer.analyze("host-1", 30.0);
            if (result.isCusumAlert() && "DOWNWARD_SHIFT".equals(result.getShiftDirection())) {
                detected = true;
                break;
            }
        }

        assertTrue(detected, "CUSUM should detect downward shift from 70 to 30");
    }

    @Test
    void testCusumStableSignal() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 4.0, 30, 20);

        boolean anyAlert = false;
        for (int i = 0; i < 100; i++) {
            double value = 50.0 + (i % 2 == 0 ? 1.0 : -1.0);
            SignalResult result = analyzer.analyze("host-1", value);
            if (result.isCusumAlert()) {
                anyAlert = true;
            }
        }

        assertFalse(anyAlert, "Minor noise should not trigger CUSUM");
    }

    @Test
    void testEntropyConstantSignal() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 100.0, 20, 20);

        SignalResult result = null;
        for (int i = 0; i < 20; i++) {
            result = analyzer.analyze("host-1", 50.0);
        }

        assertEquals(0.0, result.getEntropy(), 0.001,
                "Constant signal should have zero entropy");
    }

    @Test
    void testEntropySpreadSignal() {
        SignalAnalyzer analyzer = new SignalAnalyzer(0.3, 0.5, 100.0, 20, 20);

        SignalResult result = null;
        for (int i = 0; i < 20; i++) {
            result = analyzer.analyze("host-1", i * 5.0);
        }

        assertTrue(result.getNormalizedEntropy() > 0.8,
                "Spread values should have high normalized entropy");
    }

    @Test
    void testHostIsolation() {
        SignalAnalyzer analyzer = new SignalAnalyzer();

        for (int i = 0; i < 30; i++) {
            analyzer.analyze("host-a", 90.0);
            analyzer.analyze("host-b", 10.0);
        }

        SignalResult a = analyzer.analyze("host-a", 90.0);
        SignalResult b = analyzer.analyze("host-b", 10.0);

        assertTrue(a.getEwma() > 85.0, "Host A EWMA should track near 90");
        assertTrue(b.getEwma() < 15.0, "Host B EWMA should track near 10");
    }
}