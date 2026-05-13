package com.simulation.analytics.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simulation.analytics.AppProperties;
import com.simulation.analytics.model.*;
import com.simulation.analytics.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AnalyticsTopology {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public KStream<String, String> topology(StreamsBuilder builder, AppProperties props, ObjectMapper mapper) {
        var metricUpdateSerde = new JsonSerde<>(mapper, MetricUpdate.class);
        var stateSerde = new JsonSerde<>(mapper, AnalyticsState.class);
        var outputSerde = new JsonSerde<>(mapper, BallAnalytics.class);

        KStream<String, MetricUpdate> positionUpdates = builder
                .stream(props.getTopics().getPosition(), Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((key, json) -> {
                    try {
                        PositionEvent p = mapper.readValue(json, PositionEvent.class);
                        return java.util.List.of(
                                KeyValue.pair(p.sessionId() + "|" + p.ballId(),
                                        MetricUpdate.fromPosition(p.sessionId(), p.ballId(), p.timestampMs(),
                                                p.x(), p.y(), Math.hypot(p.vx(), p.vy())))
                        );
                    } catch (Exception e) {
                        System.err.println("Failed to parse position event: " + e.getMessage());
                        return java.util.List.of();
                    }
                });

        KStream<String, MetricUpdate> collisionUpdates = builder
                .stream(props.getTopics().getCollision(), Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((key, json) -> {
                    try {
                        CollisionEvent c = mapper.readValue(json, CollisionEvent.class);
                        return java.util.List.of(
                                KeyValue.pair(c.sessionId() + "|" + c.ballAId(),
                                        MetricUpdate.fromCollision(c.sessionId(), c.ballAId(), c.timestampMs())),
                                KeyValue.pair(c.sessionId() + "|" + c.ballBId(),
                                        MetricUpdate.fromCollision(c.sessionId(), c.ballBId(), c.timestampMs()))
                        );
                    } catch (Exception e) {
                        System.err.println("Failed to parse collision event: " + e.getMessage());
                        return java.util.List.of();
                    }
                });

        KTable<String, AnalyticsState> aggregated = positionUpdates
                .merge(collisionUpdates)
                .groupByKey(Grouped.with(Serdes.String(), metricUpdateSerde))
                .aggregate(
                        AnalyticsState::new,
                        (key, update, state) -> {
                            state.apply(update);
                            return state;
                        },
                        Materialized.<String, AnalyticsState, KeyValueStore<Bytes, byte[]>>as("analytics-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(stateSerde)
                );

        aggregated
                .toStream()
                .map((key, state) -> KeyValue.pair(key, new BallAnalytics(
                        state.sessionId, state.ballId, state.latestTimestampMs,
                        state.latestX, state.latestY, state.currentSpeed,
                        state.avgSpeed10s(), state.collisionCount30s()
                )))
                .to(props.getTopics().getAnalytics(), Produced.with(Serdes.String(), outputSerde));

        return null;
    }
}
