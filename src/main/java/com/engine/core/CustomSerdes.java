package com.engine.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerdes {

    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static <T> Serde<T> jsonSerde(Class<T> targetType) {
        return Serdes.serdeFrom(new JsonSerializer<>(targetType), new JsonDeserializer<>(targetType));
    }

    public static class JsonSerializer<T> implements Serializer<T> {
        private final Class<T> destinationClass;

        public JsonSerializer(Class<T> destinationClass) {
            this.destinationClass = destinationClass;
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) return null;
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }
    }

    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> destinationClass;

        public JsonDeserializer(Class<T> destinationClass) {
            this.destinationClass = destinationClass;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return mapper.readValue(data, destinationClass);
            } catch (Exception e) {
                // will return null on error to allow poison pill handling downstream
                // or throw SerializationException to crash the stream.
                return null;
            }
        }
    }
}