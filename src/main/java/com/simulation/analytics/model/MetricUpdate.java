package com.simulation.analytics.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricUpdate {
    public final String sessionId;
    public final String ballId;
    public final long timestampMs;
    public final Double x;
    public final Double y;
    public final Double speed;
    public final boolean collision;

    @JsonCreator
    public MetricUpdate(
            @JsonProperty("sessionId") String sessionId,
            @JsonProperty("ballId") String ballId,
            @JsonProperty("timestampMs") long timestampMs,
            @JsonProperty("x") Double x,
            @JsonProperty("y") Double y,
            @JsonProperty("speed") Double speed,
            @JsonProperty("collision") boolean collision
    ) {
        this.sessionId = sessionId;
        this.ballId = ballId;
        this.timestampMs = timestampMs;
        this.x = x;
        this.y = y;
        this.speed = speed;
        this.collision = collision;
    }

    public static MetricUpdate fromPosition(String sessionId, String ballId, long timestampMs, double x, double y, double speed) {
        return new MetricUpdate(sessionId, ballId, timestampMs, x, y, speed, false);
    }

    public static MetricUpdate fromCollision(String sessionId, String ballId, long timestampMs) {
        return new MetricUpdate(sessionId, ballId, timestampMs, null, null, null, true);
    }
}
