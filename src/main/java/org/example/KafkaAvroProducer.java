package org.example;


import org.example.avro.Order;
import org.example.avro.OrderStatus;
import org.example.avro.User;
import org.example.config.KafkaProducerConfig;
import org.example.model.UserEventProducer;
import org.example.util.SchemaRegistryUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaAvroProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAvroProducer.class);

    public static void main(String[] args) {
        KafkaProducerConfig config = new KafkaProducerConfig();
        Properties producerProps = config.getProducerProperties();

        System.out.println(producerProps);


        // Initialize Schema Registry Utility
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil("http://localhost:8081");

        try (KafkaProducer<String, User> userProducer = new KafkaProducer<>(producerProps);
             KafkaProducer<String, Order> orderProducer = new KafkaProducer<>(producerProps)) {


            // Get topic names
            String userTopic = config.getTopicName("user-events");
            String orderTopic = config.getTopicName("order-events");


            // Initialize event producers
            UserEventProducer userEventProducer = new UserEventProducer(userProducer, userTopic);

            logger.info("Starting Kafka Avro Producer...");

            // Demonstrate different scenarios
           demonstrateUserEvents(userEventProducer);
//            demonstrateOrderEvents(orderProducer, orderTopic);
         //   demonstrateSchemaRegistryOperations(schemaRegistryUtil);

            logger.info("All messages sent successfully!");

        } catch (Exception e) {
            logger.error("Error in Kafka Avro Producer", e);
        }
    }

    private static void demonstrateUserEvents(UserEventProducer userEventProducer) throws InterruptedException, ExecutionException {
        logger.info("=== Demonstrating User Events ===");

        // Send user created events
        for (int i = 1; i <= 5; i++) {
            Future<RecordMetadata> future = userEventProducer.sendUserCreatedEvent(
                    i,
                    "user" + i,
                    "user" + i + "@example.com",
                    20 + (i * 5)
            );

            // Wait for acknowledgment (optional - makes it synchronous)
            RecordMetadata metadata = future.get();
            logger.info("User {} created event acknowledged - Offset: {}", i, metadata.offset());

            Thread.sleep(1000); // Wait 1 second between messages
        }
    }

    private static void demonstrateOrderEvents(KafkaProducer<String, Order> producer, String topic) throws InterruptedException {
        logger.info("=== Demonstrating Order Events ===");

        for (int i = 1; i <= 3; i++) {
            Order order = createSampleOrder(i);
            sendOrderEvent(producer, topic, order);
            Thread.sleep(1500);
        }
    }

    private static Order createSampleOrder(int orderId) {
        // Convert BigDecimal to bytes for decimal logical type
        BigDecimal amount = new BigDecimal("99.99").add(BigDecimal.valueOf(orderId * 10));
        byte[] amountBytes = amount.unscaledValue().toByteArray();

        return Order.newBuilder()
                .setOrderId("ORDER-" + UUID.randomUUID().toString().substring(0, 8))
                .setUserId((long) orderId)
                .setAmount(ByteBuffer.wrap(amountBytes))
                .setStatus(OrderStatus.PENDING)
                .setItems(Arrays.asList("Item " + orderId + "A", "Item " + orderId + "B"))
                .setOrderDate(Instant.now().toEpochMilli())
                .build();
    }

    private static void sendOrderEvent(KafkaProducer<String, Order> producer, String topic, Order order) {
        String key = order.getOrderId().toString();
        ProducerRecord<String, Order> record = new ProducerRecord<>(topic, key, order);

        // Add headers
        record.headers().add("event_type", "order_created".getBytes());
        record.headers().add("user_id", String.valueOf(order.getUserId()).getBytes());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send order event: {}", order.getOrderId(), exception);
            } else {
                logger.info("Order event sent successfully - Order: {}, Topic: {}, Partition: {}, Offset: {}",
                        order.getOrderId(), metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private static void demonstrateSchemaRegistryOperations(SchemaRegistryUtil schemaRegistryUtil) {
        logger.info("=== Demonstrating Schema Registry Operations ===");

        try {
            // List all subjects
            logger.info("All subjects in Schema Registry: {}", schemaRegistryUtil.getAllSubjects());

            // Get schema information
            String userSubject = "user-events-value";
            if (schemaRegistryUtil.getAllSubjects().contains(userSubject)) {
                logger.info("Latest schema for {}: {}", userSubject,
                        schemaRegistryUtil.getLatestSchema(userSubject));
            }

        } catch (Exception e) {
            logger.warn("Schema Registry operations failed (this is normal if schemas haven't been registered yet): {}",
                    e.getMessage());
        }
    }
}