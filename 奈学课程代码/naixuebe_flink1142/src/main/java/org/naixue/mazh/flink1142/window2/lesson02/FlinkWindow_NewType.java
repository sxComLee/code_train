package org.naixue.mazh.flink1142.window2.lesson02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Non Keyed Window 和 Keyed Window
 */
public class FlinkWindow_NewType {

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

        // TODO_MA 马中华 注释： Non keyed Stream
        /*AllWindowedStream<Tuple2<String, Integer>, TimeWindow> nonkeyedStream = stream.timeWindowAll(Time.seconds(3));
        nonkeyedStream.sum(1)
                .print();*/

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Keyed Stream  滚动窗口  2s
         *  .timeWindow(Time.seconds(2))  == .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
         */
        stream.keyBy(value -> value.f0)
                // TODO_MA 马中华 注释： 指定生成 window 的规则:  WindowAssigner
                // TODO_MA 马中华 注释： WindowAssigner 有比较常用的四个子类
                // TODO_MA 马中华 注释： 基于 EventTime 每隔 5s 做一次计算
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // TODO_MA 马中华 注释： 每隔 4s 执行过去 6s 内数据的计算
                // .window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(4))
                // .countWindow(100)
                // .countWindow(2,1)
                // .timeWindow(Time.seconds(3),Time.seconds(1))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        stream.keyBy("0")
                .timeWindow(Time.seconds(10))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        stream.keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        stream.keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(2))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        stream.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        stream.keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释：
        env.execute("word count");
    }
}

