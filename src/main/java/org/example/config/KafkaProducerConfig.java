package org.example.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerConfig {

    private static final String CONFIG_FILE = "application.properties";
    private Properties configProps;

    public KafkaProducerConfig() {
        loadConfiguration();
    }

    private void loadConfiguration() {
        configProps = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                throw new RuntimeException("Unable to find " + CONFIG_FILE);
            }
            configProps.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading configuration", e);
        }
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();

        // Basic Kafka Configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configProps.getProperty("kafka.bootstrap.servers"));

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry
        props.put("schema.registry.url", configProps.getProperty("kafka.schema.registry.url"));
        props.put("auto.register.schemas", configProps.getProperty("schema.registry.auto.register.schemas"));
        props.put("use.latest.version", configProps.getProperty("schema.registry.use.latest.version"));

        // Producer Performance & Reliability
        props.put(ProducerConfig.ACKS_CONFIG, configProps.getProperty("kafka.producer.acks"));
        props.put(ProducerConfig.RETRIES_CONFIG, configProps.getProperty("kafka.producer.retries"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, configProps.getProperty("kafka.producer.batch.size"));
        props.put(ProducerConfig.LINGER_MS_CONFIG, configProps.getProperty("kafka.producer.linger.ms"));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, configProps.getProperty("kafka.producer.buffer.memory"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                configProps.getProperty("kafka.producer.enable.idempotence"));

        // Additional Configuration for Production
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return props;
    }

    public String getTopicName(String topicKey) {
        return configProps.getProperty("kafka.topics." + topicKey);
    }
}