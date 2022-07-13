package org.naixue.mazh.flink1142.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每隔 5秒 计算最近 10秒 的单词次数
 */
public class TimeWindowWordCount01_TimeWindow {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO_MA 马中华 注释： 老 API 启用 Time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // TODO_MA 马中华 注释： 获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 数据的处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = dataStream.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                                String[] fields = line.split(",");
                                for (String word : fields) {
                                    out.collect(Tuple2.of(word, 1));
                                }
                            }
                        })
                .keyBy(tuple -> tuple.f0)
                // TODO_MA 马中华 注释： 老版本的使用方式
                // TODO_MA 马中华 注释： env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                // .timeWindow(Time.seconds(10), Time.seconds(5))
                // TODO_MA 马中华 注释： 新版本的使用方式： WindowAssigner
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1);

        // TODO_MA 马中华 注释：
        resultDS.print()
                .setParallelism(1); //数据的输出

        // TODO_MA 马中华 注释： 启动程序
        env.execute("WindowWordCount");
    }
}