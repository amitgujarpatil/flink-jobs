package org.example;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.avro.Schema;
import org.example.pojo.Todo;
import org.apache.flink.configuration.Configuration;

public class Main {

    private static final String FileName = "todos.json";
    private  static  final String TOPIC = "amit";
    private static final String SCHEMA_REGISTRY_URL =  "http://localhost:8081";

    public static  void main(String[] args) throws  Exception {

        try{

            Configuration conf = new Configuration();
            conf.setString("rest.port", "8084"); // Use port 8082 instead


            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
            env.setParallelism(1);

           // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            AvroDeserializationSchema<GenericRecord> deserializationSchema = Main.getAvroDeserializationSchema(TOPIC, SCHEMA_REGISTRY_URL);

            if(deserializationSchema == null){
                throw  new Exception("Avro Schema registry not found for given topic");
            }

            // Use specific initializer that combines committed offsets with a fallback
            KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("amit")
                    .setGroupId("amit-g")
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setValueOnlyDeserializer(deserializationSchema)
                    .build();

            DataStream<GenericRecord> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

            stream.keyBy( key -> {
                return key.get("age");
            }).print();



            env.execute("amit");

        }catch (Exception e){
            System.out.println(e);
        }



    }


    public static AvroDeserializationSchema<GenericRecord> getAvroDeserializationSchema(String topic, String schemaRegistryUrl){
        try{

            String schemaSubject = topic + "-value";

            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
            String schema = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject).getSchema();
            Schema parsedSchema = new Schema.Parser().parse(schema);

            return ConfluentRegistryAvroDeserializationSchema.forGeneric(parsedSchema,schemaRegistryUrl);
        } catch (Exception e) {
           return null;
        }
    }

}