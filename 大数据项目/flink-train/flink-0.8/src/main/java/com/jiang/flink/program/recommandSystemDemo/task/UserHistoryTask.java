package com.jiang.flink.program.recommandSystemDemo.task;

import com.jiang.flink.program.recommandSystemDemo.map.ProductPortraitMapFunction;
import com.jiang.flink.program.recommandSystemDemo.map.UserHistoryMapFunction;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName UserHistoryTask
 * @Description TODO
 *  用户-产品浏览历史 -> 实现基于协同过滤的推荐逻辑
 *
 * 通过Flink去记录用户浏览过这个类目下的哪些产品,为后面的基于Item的协同过滤做准备 实时的记录用户的评分到Hbase中,为后续离线处理做准备.
 *
 * 数据存储在Hbase的p_history表
 * @Author jiang.li
 * @Date 2020-01-10 13:14
 * @Version 1.0
 */
public class UserHistoryTask {

    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool,"history");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("con", new SimpleStringSchema(), properties);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        dataSource.map(new UserHistoryMapFunction());

        env.execute("User Product History");
    }
}
