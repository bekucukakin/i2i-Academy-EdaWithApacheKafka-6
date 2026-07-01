package com.i2iacademy.kafka.producer;

import com.i2iacademy.kafka.model.OrderEvent;
import com.i2iacademy.kafka.serde.OrderEventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class ProducerApp {

    private static final String TOPIC = "order-events";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderEventSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        String[] products = {"Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"};

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String orderId = UUID.randomUUID().toString();
                String product = products[i % products.length];
                OrderEvent event = new OrderEvent(orderId, product, i, 19.99 * i);

                ProducerRecord<String, OrderEvent> record =
                        new ProducerRecord<>(TOPIC, orderId, event);

                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Send failed: " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "Sent -> topic=%s, partition=%d, offset=%d, event=%s%n",
                                metadata.topic(), metadata.partition(), metadata.offset(), event);
                    }
                });

                Thread.sleep(1000);
            }
            producer.flush();
        }

        System.out.println("Producer finished.");
    }
}