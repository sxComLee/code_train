package com.jiang.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @ClassName KafkaProducerAnalysis
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-23 21:26
 * @Version 1.0
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "client-id");
        prop.put(ProducerConfig.RETRIES_CONFIG, 5);

        return prop;
    }



    public static void main(String[] args) {
        Properties prop = initConfig();
        KafkaProducer<String, String> produce = new KafkaProducer<>(prop);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic,"hello-kafka");

        produce.send(record);

        try {
            Future<RecordMetadata> future = produce.send(record);
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        produce.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(recordMetadata != null){

                }
                if(e != null){

                }
            }
        });

        produce.close();
    }

}
