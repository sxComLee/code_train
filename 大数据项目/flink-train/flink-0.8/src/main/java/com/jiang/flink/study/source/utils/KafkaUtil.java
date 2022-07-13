package com.jiang.flink.study.source.utils;

import com.jiang.flink.study.common.model.MetricEvent;
import com.jiang.flink.study.common.util.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName KafkaUtil
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-16 14:25
 * @Version 1.0
 */
public class KafkaUtil {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "metric";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setTimestamp(System.currentTimeMillis());
        metricEvent.setName("mem");

        HashMap<String,String> tags = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metricEvent.setTags(tags);
        metricEvent.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(metricEvent));
        producer.send(record);
        System.out.println("发送数据: " + GsonUtil.toJson(metricEvent));

        producer.flush();

    }

}
