package org.example.connectors;

import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Avro {

    public Schema getAvroSchema(String topic, Properties schemaRegistryProperties) {
        try {

            String schemaRegistryUrl = schemaRegistryProperties.get("schemaRegistryUrl").toString();
            String schemaRegistryUser = schemaRegistryProperties.get("schemaRegistryUser").toString();
            String schemaRegistryPassword = schemaRegistryProperties.get("schemaRegistryPassword").toString();

            Map<String, Object> authConfigs = new HashMap<>();
            authConfigs.put("basic.auth.credentials.source", "USER_INFO");
            authConfigs.put("basic.auth.user.info", schemaRegistryUser + ":" + schemaRegistryPassword);

            String schemaSubject = topic + "-value";

            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
            String schemaString = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject).getSchema();

            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            System.out.println("" + e);
            return new Schema.Parser().parse("");
        }
    }

    public ConfluentRegistryAvroDeserializationSchema getAvroDeserializer(String topic, Properties schemaRegistryProperties) {
        String schemaRegistryUrl = schemaRegistryProperties.get("schemaRegistryUrl").toString();
        String schemaRegistryUser = schemaRegistryProperties.get("schemaRegistryUser").toString();
        String schemaRegistryPassword = schemaRegistryProperties.get("schemaRegistryPassword").toString();

        Map<String, Object> schemaRegistryConfigs = new HashMap<>();
        schemaRegistryConfigs.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        schemaRegistryConfigs.put(KafkaAvroDeserializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        schemaRegistryConfigs.put(KafkaAvroDeserializerConfig.USER_INFO_CONFIG, schemaRegistryUser + ":" + schemaRegistryPassword);
        try {
            Schema schema =  getAvroSchema(topic, schemaRegistryProperties);

            ConfluentRegistryAvroDeserializationSchema<GenericRecord> deserializationSchema = ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schemaRegistryUrl, schemaRegistryConfigs);
            return deserializationSchema;
        } catch (Exception e) {
            System.out.println("" + e);
            ConfluentRegistryAvroDeserializationSchema<GenericRecord> deserializationSchema = ConfluentRegistryAvroDeserializationSchema.forGeneric(Schema.parse(""), schemaRegistryUrl, schemaRegistryConfigs);
            return deserializationSchema;
        }
    }
}