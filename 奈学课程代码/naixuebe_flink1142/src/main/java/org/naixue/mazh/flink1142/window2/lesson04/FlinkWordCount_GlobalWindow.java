package org.naixue.mazh.flink1142.window2.lesson04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 单词每出现三次统计一次,统计最近三次的数据
 */
public class FlinkWordCount_GlobalWindow {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] fields = line.split(",");
                        for (String word : fields) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                });

        // TODO_MA 马中华 注释：
        stream.keyBy(tuple -> tuple.f0)
                // TODO_MA 马中华 注释： 指定使用 global window
                .window(GlobalWindows.create())
                // 这个是 flink 提供的
                // TODO_MA 马中华 注释： 调用自定义的 Trigger
                // TODO_MA 马中华 注释： 每隔 3条元素 执行一次计算
                // TODO_MA 马中华 注释： CountTrigger 和 TimeTrigger 是 FLink 提供的常用实现
                .trigger(CountTrigger.of(3))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        env.execute("FlinkWordCount_GlobalWindow");
    }
}
