package org.lij.flink1_14_2.day01.wordCount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-24 22:27
 */
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        executionEnvironment.setParallelism(1);

        // 加载数据源获取 source 对象。。。DataStreamSource 是一个 DataStream 对象
        DataStreamSource<String> sourceStream = executionEnvironment.socketTextStream("localhost", 6789);

        // 执行逻辑处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDs = sourceStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    // 将结果输出
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).setParallelism(3);

        // 分组结合求结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS
                = wordAndOneDs.keyBy(0).sum(1).setParallelism(4);
        // 结果输出
        resultDS.print().setParallelism(5);

        // 提交执行
        executionEnvironment.execute(" flink streaming wordCount");

    }
}
