package com.jiang.flink.program.recommandSystemDemo.task;

import com.jiang.flink.program.recommandSystemDemo.map.LogMapFunction;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName LogTask
 * @Description TODO
 * 从Kafka接收的数据直接导入进Hbase事实表,保存完整的日志log,日志中包含了用户Id,用户操作的产品id,操作时间,行为(如购买,点击,推荐等).
 *
 * 数据按时间窗口统计数据大屏需要的数据,返回前段展示
 *
 * 数据存储在Hbase的con表
 * @Author jiang.li
 * @Date 2020-01-10 11:23
 * @Version 1.0
 */
public class LogTask {
    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("con", new SimpleStringSchema(), properties);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        dataSource.map(new LogMapFunction());

        env.execute("Log message receive");
    }
}
