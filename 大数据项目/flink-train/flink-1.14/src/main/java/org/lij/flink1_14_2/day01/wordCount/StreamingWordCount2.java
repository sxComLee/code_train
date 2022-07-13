package org.lij.flink1_14_2.day01.wordCount;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description:
 *注释： Flink-1.14 版本的 流式 wordcount 入门案例
 *  *  测试的时候，运行：nc -lk 6789
 * @author lij
 * @date 2022-01-24 22:47
 */
public class StreamingWordCount2 {
    public static void main(String[] args) throws Exception{
        // getExecutionEnvironment 和 createExecutionEnvironment 区别
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 获取数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // 进行数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result
                = source.flatMap(new SplitFunction())
                .keyBy(0)
                .sum(1);

        // 进行结果输出
        result.print().setParallelism(4);

        // 任务开始执行
        env.execute("flink streaming wordCount");

    }

    public static class SplitFunction extends RichFlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split(" ");
            for (String word: words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}


