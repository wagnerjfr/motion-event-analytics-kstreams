package com.simulation.analytics.serde;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;


public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper;
    private final Class<T> type;

    public JsonSerde(ObjectMapper mapper, Class<T> type) {
        this.mapper = mapper;
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (JacksonException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return mapper.readValue(data, type);
            } catch (JacksonException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
