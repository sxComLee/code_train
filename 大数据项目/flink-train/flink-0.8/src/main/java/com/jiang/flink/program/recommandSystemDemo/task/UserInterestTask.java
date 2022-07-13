package com.jiang.flink.program.recommandSystemDemo.task;

import com.jiang.flink.program.recommandSystemDemo.map.GetLogFunction;
import com.jiang.flink.program.recommandSystemDemo.map.UserHistoryMapFunction;
import com.jiang.flink.program.recommandSystemDemo.map.UserHistoryWithInterestMapFunction;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName UserInterestTask
 * @Description TODO
 *  用户-兴趣 -> 实现基于上下文的推荐逻辑
 *
 * 根据用户对同一个产品的操作计算兴趣度,计算规则通过操作间隔时间(如购物 - 浏览 < 100s)则判定为一次兴趣事件
 *      通过Flink的ValueState实现,如果用户的操作Action=3(收藏),则清除这个产品的state,如果超过100s没有出现Action=3的事件,也会清除这个state
 *
 * 数据存储在Hbase的u_interest表
 * @Author jiang.li
 * @Date 2020-01-10 13:39
 * @Version 1.0
 */
public class UserInterestTask {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool,"interest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("con", new SimpleStringSchema(), properties);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        dataSource.map(new GetLogFunction())
                .keyBy("userId")
                .map(new UserHistoryWithInterestMapFunction());


        env.execute("User Product History");
    }
}
