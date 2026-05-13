package com.simulation.analytics.model;

public record BallAnalytics(
        String sessionId,
        String ballId,
        long timestampMs,
        double latestX,
        double latestY,
        double currentSpeed,
        double avgSpeed10s,
        long collisionsCount30s
) {
}
