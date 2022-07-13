package com.jiang.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @ClassName KafkaConsumerAnalysis
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-07-25 16:03
 * @Version 1.0
 */
public class KafkaConsumerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "tipic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig(){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return prop;
    }

    public static void main(String[] args) {
        Properties prop = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);


        List<String> topicList = Arrays.asList(topic);

        consumer.subscribe(topicList);
//        consumer.subscribe(Pattern.compile("topic_*"));

        consumer.assign(new ArrayList<TopicPartition>());

        try{
            while(isRunning.get()){
                ConsumerRecords<String, String> records = consumer.poll(1000);
                //将所有的数据进行消费
                for (ConsumerRecord<String,String> cr: records) {
                    System.out.println(cr.topic()+cr.partition()+cr.offset());
                    System.out.println(cr.key()+cr.value());
                    //另一种提交当前消费offset的方式，一次拉取多次提交
                    long offset = cr.offset();
                    TopicPartition topicPartition = new TopicPartition(cr.topic(), cr.partition());
                    consumer.commitSync(Collections.singletonMap(topicPartition,new OffsetAndMetadata(offset)));
                }

                //将所有的数据进行消费的另一种方式
                for (TopicPartition tp : records.partitions()) {
                    for (ConsumerRecord<String,String> cr :records.records(tp) ) {
                        System.out.println(cr.topic()+cr.partition()+cr.offset());
                        System.out.println(cr.key()+cr.value());

                    }

                    //提交当前消息消费的位置 offset，一次拉取一次提交
                    OffsetAndMetadata committed = consumer.committed(tp);

                    //获取下一次拉取消息的位置 position
                    long position = consumer.position(tp);
                }
                //按照主题维度进行消费
                for ( String topic : topicList) {
                    for ( ConsumerRecord<String,String> cr : records.records(topic) ) {
                        System.out.println(cr.topic()+cr.partition()+cr.offset());
                        System.out.println(cr.key()+cr.value());

                    }
                }
            }
            // 取消订阅
            consumer.unsubscribe();


        }finally {
            //关闭消费者
            consumer.close();
        }

    }


}
