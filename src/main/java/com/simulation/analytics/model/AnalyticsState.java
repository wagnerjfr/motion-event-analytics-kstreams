package com.simulation.analytics.model;

import java.util.ArrayDeque;
import java.util.Deque;

public class AnalyticsState {
    public String sessionId;
    public String ballId;
    public long latestTimestampMs;
    public double latestX;
    public double latestY;
    public double currentSpeed;
    public Deque<SpeedSample> speedSamples = new ArrayDeque<>();
    public Deque<Long> collisionTimestamps = new ArrayDeque<>();
    public double speedSum10s;

    public void apply(MetricUpdate update) {
        sessionId = update.sessionId;
        ballId = update.ballId;
        latestTimestampMs = update.timestampMs;

        if (update.collision) {
            collisionTimestamps.addLast(update.timestampMs);
        } else {
            latestX = update.x;
            latestY = update.y;
            currentSpeed = update.speed;
            speedSamples.addLast(new SpeedSample(update.timestampMs, update.speed));
            speedSum10s += update.speed;
        }

        trimOldSamples();
    }

    public double avgSpeed10s() {
        return speedSamples.isEmpty() ? 0.0 : speedSum10s / speedSamples.size();
    }

    public long collisionCount30s() {
        return collisionTimestamps.size();
    }

    private void trimOldSamples() {
        trimSpeedSamples();
        trimCollisionTimestamps();
    }

    private void trimSpeedSamples() {
        long threshold = latestTimestampMs - 10_000;
        while (!speedSamples.isEmpty() &&                 speedSamples.peekFirst().timestampMs() < threshold) {
            SpeedSample removed = speedSamples.removeFirst();
            speedSum10s -= removed.speed();
        }
    }

    private void trimCollisionTimestamps() {
        long threshold = latestTimestampMs - 30_000;
        while (!collisionTimestamps.isEmpty() && collisionTimestamps.peekFirst() < threshold) {
            collisionTimestamps.removeFirst();
        }
    }
}
