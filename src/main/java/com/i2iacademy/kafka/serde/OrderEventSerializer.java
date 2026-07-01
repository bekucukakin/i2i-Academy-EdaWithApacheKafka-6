package com.i2iacademy.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.i2iacademy.kafka.model.OrderEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UncheckedIOException;

public class OrderEventSerializer implements Serializer<OrderEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, OrderEvent data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new UncheckedIOException(
                    "Failed to serialize OrderEvent to JSON, topic=" + topic,
                    new java.io.IOException(e));
        }
    }
}