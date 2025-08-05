package org.example.pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.config.KafkaProducerConfig;
import org.example.pojo.Todo;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaPublisher {

    private String topic = "amit";
    private KafkaProducer<String,String> producer;

    public KafkaPublisher() {

        try{
            Properties props = new Properties();

            // Basic Kafka Configuration
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            // Serializers
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
            // Additional Configuration for Production
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

            producer = new KafkaProducer<String,String>(props);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    public Future<RecordMetadata> Publish(String key,String value){


       return this.producer.send(
                new ProducerRecord<String,String>(this.topic, key, value),
                (event, ex) -> {
                    if (ex != null){
                        System.out.println(ex);
                        ex.printStackTrace();
                    } else
                        System.out.println(event.offset());
                });

    }

}
