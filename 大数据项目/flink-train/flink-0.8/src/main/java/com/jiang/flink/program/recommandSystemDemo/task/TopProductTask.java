package com.jiang.flink.program.recommandSystemDemo.task;

import com.jiang.flink.program.recommandSystemDemo.agg.CountAgg;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import com.jiang.flink.program.recommandSystemDemo.map.TopProductMapFunction;
import com.jiang.flink.program.recommandSystemDemo.map.UserPortraitMapFunction;
import com.jiang.flink.program.recommandSystemDemo.top.TopNHotItems;
import com.jiang.flink.program.recommandSystemDemo.window.WindowResultFunction;
import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.common.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 * @ClassName TopProductTask
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 17:03
 * @Version 1.0
 */
public class TopProductTask {
    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool,"topProuct");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("con", new SimpleStringSchema(), properties);

        DataStreamSource<String> dataSource = env.addSource(consumer);

        dataSource
                .map(new TopProductMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEntity>() {
                    @Override
                    public long extractAscendingTimestamp(LogEntity element) {
                        return element.getTime();
                    }
                }).keyBy("productId")
                //
                .timeWindow(Time.seconds(60),Time.seconds(5))
                .aggregate(new CountAgg(),new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(5));

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(parameterTool.get("redis.host"))
//				.setPort(Property.getIntValue("redis.port"))
//				.setDatabase(Property.getIntValue("redis.db"))
                .build();


//        topN.addSink(new RedisSink<>(conf,topN));
        env.execute("Top N");
    }

}
