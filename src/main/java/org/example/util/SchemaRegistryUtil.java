package org.example.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SchemaRegistryUtil {

    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryUtil.class);
    private final SchemaRegistryClient schemaRegistryClient;

    public SchemaRegistryUtil(String schemaRegistryUrl) {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    /**
     * Register a schema for a subject
     */
    public int registerSchema(String subject, Schema schema) {
        try {
            int schemaId = schemaRegistryClient.register(subject, schema);
            logger.info("Registered schema for subject: {} with ID: {}", subject, schemaId);
            return schemaId;
        } catch (IOException | RestClientException e) {
            logger.error("Failed to register schema for subject: {}", subject, e);
            throw new RuntimeException("Schema registration failed", e);
        }
    }

    /**
     * Get latest schema for a subject
     */
    public String getLatestSchema(String subject) {
        try {
            return schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        } catch (IOException | RestClientException e) {
            logger.error("Failed to get latest schema for subject: {}", subject, e);
            throw new RuntimeException("Failed to get schema", e);
        }
    }

    /**
     * List all subjects
     */
    public List<String> getAllSubjects() {
        try {
            return new ArrayList<>(schemaRegistryClient.getAllSubjects());
        } catch (IOException | RestClientException e) {
            logger.error("Failed to get all subjects", e);
            throw new RuntimeException("Failed to get subjects", e);
        }
    }

    /**
     * Check if schema is compatible
     */
    public boolean isSchemaCompatible(String subject, Schema schema) {
        try {
            return schemaRegistryClient.testCompatibility(subject, schema);
        } catch (IOException | RestClientException e) {
            logger.error("Failed to check schema compatibility for subject: {}", subject, e);
            return false;
        }
    }
}