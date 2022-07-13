package com.jiang.flink.program.recommandSystemDemo.task;

import com.jiang.flink.program.recommandSystemDemo.map.ProductPortraitMapFunction;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName ProductProtaritTask
 * @Description TODO
 *  产品画像记录 -> 实现基于标签的推荐逻辑
 *
 * 用两个维度记录产品画像,一个是喜爱该产品的年龄段,另一个是性别
 *
 * 数据存储在Hbase的prod表
 * @Author jiang.li
 * @Date 2020-01-10 11:55
 * @Version 1.0
 */
public class ProductProtaritTask {
    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool,"ProductPortrait");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("con", new SimpleStringSchema(), properties);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        dataSource.map(new ProductPortraitMapFunction());

        env.execute("Product Portrait");
    }
}
