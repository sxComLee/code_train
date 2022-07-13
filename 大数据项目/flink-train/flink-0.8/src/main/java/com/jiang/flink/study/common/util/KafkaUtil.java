package com.jiang.flink.study.common.util;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Properties;

/**
 * @ClassName KafkaUtil
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-07 09:44
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class KafkaUtil {
    String groupId = "";
    String bootstrapServer = "";
    String topics = "";


    public Properties getKafkaProperties(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServer);

        props.setProperty("group.id", groupId);

        props.put("session.timeout.ms", 30000);       // 如果其超时，将会可能触发rebalance并认为已经死去，重新选举Leader
//        props.put("enable.auto.commit", "true");      // 开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交时间
        props.put("auto.offset.reset","earliest"); // 从最早的offset开始拉取，latest:从最近的offset开始消费
//        props.put("client.id", clientId); // 发送端id,便于统计
        props.put("max.poll.records","100"); // 每次批量拉取条数
        props.put("max.poll.interval.ms","1000"); //拉取间隔毫秒数
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //key序列化方式
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value序列化方式
//        props.put("isolation.level","read_committed"); // 设置隔离级别

        return props;
    }


}
