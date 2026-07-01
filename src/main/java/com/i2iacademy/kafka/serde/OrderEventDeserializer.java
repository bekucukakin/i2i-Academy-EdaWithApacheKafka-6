package com.i2iacademy.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.i2iacademy.kafka.model.OrderEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UncheckedIOException;

public class OrderEventDeserializer implements Deserializer<OrderEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OrderEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, OrderEvent.class);
        } catch (Exception e) {
            throw new UncheckedIOException(
                    "Failed to deserialize OrderEvent from JSON, topic=" + topic,
                    new java.io.IOException(e));
        }
    }
}