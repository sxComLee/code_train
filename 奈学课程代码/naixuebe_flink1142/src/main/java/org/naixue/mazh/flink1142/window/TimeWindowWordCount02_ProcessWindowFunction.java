package org.naixue.mazh.flink1142.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 每隔 5秒 计算最近 10秒 单词出现的次数
 *  窗口开始时间：20:17:03
 *  窗口结束时间：20:17:13
 *  -
 *  窗口开始时间：20:17:08
 *  窗口结束时间：20:17:18
 */
public class TimeWindowWordCount02_ProcessWindowFunction {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
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
                // .timeWindow(Time.seconds(10), Time.seconds(5))
                // TODO_MA 马中华 注释： 每隔 5s 计算过去 10s内 数据的结果。用的是 ProcessingTime
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //porcess foreach
                // .sum(1);
                .process(new SumProcessFunction());

        // TODO_MA 马中华 注释：
        resultDS.print()
                .setParallelism(1);

        // TODO_MA 马中华 注释：
        env.execute("WindowWordCountAndTime");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        FastDateFormat dataformat = FastDateFormat.getInstance("HH:mm:ss");

        // TODO_MA 马中华 注释： 第三个参数：allElements 就是窗口内的所有数据
        // TODO_MA 马中华 注释： 触发一个 window 执行的时候，其实就是调用 process 方法一次
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {

            System.out.println("=========触发了一次窗口============================================");

            System.out.println("当前系统时间：" + dataformat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间：" + dataformat.format(context.currentProcessingTime()));

            System.out.println("窗口开始时间：" + dataformat.format(context.window().getStart()));
            System.out.println("窗口结束时间：" + dataformat.format(context.window().getEnd()));

            // TODO_MA 马中华 注释： 实现了 sum 的效果
            int count = 0;
            for (Tuple2<String, Integer> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }
}