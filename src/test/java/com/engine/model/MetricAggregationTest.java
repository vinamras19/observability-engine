package com.engine.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricAggregationTest {

    @Test
    void testAggregationLogic() {
        MetricAggregation agg = new MetricAggregation();

        agg.add(new Metric("host-1", "cpu", 10.0));
        agg.add(new Metric("host-1", "cpu", 20.0));
        agg.add(new Metric("host-1", "cpu", 30.0));

        assertEquals(3, agg.getCount(), "Count should be 3");
        assertEquals(60.0, agg.getSum(), 0.001, "Sum should be 60.0");
        assertEquals(20.0, agg.getAverage(), 0.001, "Average should be 20.0");
        assertEquals(10.0, agg.getMin(), 0.001, "Min should be 10.0");
        assertEquals(30.0, agg.getMax(), 0.001, "Max should be 30.0");
    }

    @Test
    void testEmptyAggregationProtection() {
        MetricAggregation agg = new MetricAggregation();

        assertEquals(0.0, agg.getAverage(), "Empty average should be 0.0");
        assertEquals(0.0, agg.getMin(), "Empty min should be 0.0");
        assertEquals(0.0, agg.getMax(), "Empty max should be 0.0");
        assertEquals("NORMAL", agg.isBreaching(85.0) ? "BREACH" : "NORMAL", "Empty state should not breach");
    }

    @Test
    void testBreachDetection() {
        MetricAggregation agg = new MetricAggregation();

        agg.add(new Metric("host-1", "cpu", 91.0));

        assertTrue(agg.isBreaching(85.0), "Should detect breach above 85.0");
        assertEquals("CRITICAL", agg.getSeverity(), "Severity should be CRITICAL for 91.0");
    }
}