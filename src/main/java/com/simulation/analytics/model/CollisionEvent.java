package com.simulation.analytics.model;

public record CollisionEvent(
        String sessionId,
        String ballAId,
        String ballBId,
        long timestampMs,
        double x,
        double y,
        double relativeSpeed
) {
}
