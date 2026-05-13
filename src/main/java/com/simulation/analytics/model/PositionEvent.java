package com.simulation.analytics.model;

public record PositionEvent(
        String sessionId,
        String ballId,
        long timestampMs,
        double x,
        double y,
        double vx,
        double vy
) {
}
