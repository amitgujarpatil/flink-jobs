package org.example;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.pubsub.KafkaPublisher;
import java.util.concurrent.Future;

public class Playground {

    public static void main(String[] args) throws  Exception {

        KafkaPublisher publisher = new KafkaPublisher();


        for(int i=0; i < 100; i++){
            String msg = String.format("Msg: value - %d",i);
            String key = "user-22";
            Future<RecordMetadata> res = publisher.Publish(key,msg);
            res.get();
        }

        System.out.println("published");

    }
}
