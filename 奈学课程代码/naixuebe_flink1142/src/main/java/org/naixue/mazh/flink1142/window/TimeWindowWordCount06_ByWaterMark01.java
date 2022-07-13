package org.naixue.mazh.flink1142.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每隔 5秒 计算最近 10秒 的单词次数 乱序
 */
public class TimeWindowWordCount06_ByWaterMark01 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.addSource(new TestSource());

        // TODO_MA 马中华 注释：
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] fields = s.split(",");
                        //拆分字段
                        collector.collect(Tuple2.of(fields[0], Long.valueOf(fields[1])));
                    }
                })

                // TODO_MA 马中华 注释： 如果基于 evnetTime 和 water 去实现乱序数据的处理
                .assignTimestampsAndWatermarks(
                        // TODO_MA 马中华 注释： 指定 watermark 的规则
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                // TODO_MA 马中华 注释： 指定 eventTime 的定义
                                .withTimestampAssigner((ctx) -> new TimeStampExtractor())) //指定时间字段

                .keyBy(tuple -> tuple.f0) //指定分组的字段
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //滑动窗口
                .process(new SumProcessFunction()) //计算结果
                .print();

        env.execute("WindowWordCountAndTime");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 定义 Watermark
     *  WatermarkGenerator ： watermark 生成器
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        // TODO_MA 马中华 注释： 每次接收到一条数据，就执行一次处理
        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
            //  System.out.println(eventTimestamp);
        }

        // TODO_MA 马中华 注释： 定期发送 watermark
        // TODO_MA 马中华 注释： BufferOrEvent （Flink 的数据流： 待计算的数据，嵌入的辅助数据：CheckpointBarrier + Watermark）
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // TODO_MA 马中华 注释： 指定可以延迟5s
            output.emitWatermark(new Watermark(System.currentTimeMillis() - 5000));
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 用来从数据记录中指定 eventTime 字段
     */
    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义数据源
     */
    public static class TestSource implements SourceFunction<String> {

        FastDateFormat dateformat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> cxt) throws Exception {

            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }

            System.out.println("当前时间：" + dateformat.format(System.currentTimeMillis()));
            // TODO_MA 马中华 注释： 21:39:20

            // TODO_MA 马中华 注释： 13s 输出一条数据
            TimeUnit.SECONDS.sleep(13);
            String event = "hadoop," + System.currentTimeMillis();
            String event1 = event;
            cxt.collect(event);       // TODO_MA 马中华 注释：  21:39:33

            // TODO_MA 马中华 注释： 21:39:35 本应该触发的 window 计算，延迟到了 21:39:40 的时候才执行
            // TODO_MA 马中华 注释： 这个 (hadoop,2) 结果是在 21:39:40
            // TODO_MA 马中华 注释： (hadoop,3) 在 21:39:45 的时候输出
            // TODO_MA 马中华 注释： (hadoop,1) 在 21:39:50 的时候输出

            // TODO_MA 马中华 注释： 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("hadoop," + System.currentTimeMillis());

            // TODO_MA 马中华 注释： 本该 13s 输出的一条数据，延迟到 19s 的时候，才输出
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event1);

            TimeUnit.SECONDS.sleep(3000);
        }

        @Override
        public void cancel() {

        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 窗口的处理逻辑，自定义求和
     */
    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int count = 0;
            for (Tuple2<String, Long> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }

    }
}
