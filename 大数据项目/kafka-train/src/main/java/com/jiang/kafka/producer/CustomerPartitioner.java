package com.jiang.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName CustomerPartitioner
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-24 20:22
 * @Version 1.0
 */
public class CustomerPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer num = cluster.partitionCountForTopic(topic);
        if(null == keyBytes){
            return counter.getAndIncrement() % num;
        }else{
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }


    public static Properties initPartitionConfig(){
        String brokerList = "localhost:9092";

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartitioner.class.getName());
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "client-id");
        prop.put(ProducerConfig.RETRIES_CONFIG, 5);
        return prop;
    }
}
