package org.example.model;


import org.example.avro.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class UserEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(UserEventProducer.class);
    private final KafkaProducer<String, User> producer;
    private final String topicName;

    public UserEventProducer(KafkaProducer<String, User> producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public Future<RecordMetadata> sendUserCreatedEvent(long userId, String username, String email, int age) {
        // Create metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("source", "user-service");
        metadata.put("event_type", "user_created");
        metadata.put("version", "1.0");

        // Build User object using Avro generated class
        User user = User.newBuilder()
                .setId(userId)
                .setUsername(username)
                .setEmail(email)
                .setAge(age)
                .setIsActive(true)
                .setCreatedAt(Instant.now().toEpochMilli())
                .setMetadata(metadata)
                .build();

        return sendUserEvent(user, "user_created");
    }

    public Future<RecordMetadata> sendUserUpdatedEvent(User user) {
        // Update metadata
        Map<String, String> metadata = new HashMap<>(user.getMetadata());
        metadata.put("event_type", "user_updated");
        metadata.put("updated_at", String.valueOf(Instant.now().toEpochMilli()));

        User updatedUser = User.newBuilder(user)
                .setMetadata(metadata)
                .build();

        return sendUserEvent(updatedUser, "user_updated");
    }

    private Future<RecordMetadata> sendUserEvent(User user, String eventType) {
        String key = "user-" + user.getId();
        ProducerRecord<String, User> record = new ProducerRecord<>(topicName, key, user);

        // Add headers
        record.headers().add("event_type", eventType.getBytes());
        record.headers().add("timestamp", String.valueOf(Instant.now().toEpochMilli()).getBytes());

        logger.info("Sending {} event for user: {}", eventType, user.getId());

        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send {} event for user: {}", eventType, user.getId(), exception);
            } else {
                logger.info("Successfully sent {} event - Topic: {}, Partition: {}, Offset: {}, User: {}",
                        eventType, metadata.topic(), metadata.partition(), metadata.offset(), user.getId());
            }
        });
    }
}
