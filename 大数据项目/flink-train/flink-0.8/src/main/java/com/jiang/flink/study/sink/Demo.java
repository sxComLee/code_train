package com.jiang.flink.study.sink;

import com.alibaba.fastjson.JSONObject;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import com.jiang.flink.study.sink.bean.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

import static com.jiang.flink.study.common.constant.PropertiesConstants.*;

/**
 * @ClassName Demo
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-07 19:25
 * @Version 1.0
 */
public class Demo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties prop = KafkaConfigUtil.buildKafkaProps(parameterTool);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(parameterTool.get(METRICS_TOPIC)
                , new SimpleStringSchema()
                , prop);
        env.addSource(consumer).setParallelism(1)
                .map(x -> JSONObject.parseObject(x, Student.class));
    }
}
