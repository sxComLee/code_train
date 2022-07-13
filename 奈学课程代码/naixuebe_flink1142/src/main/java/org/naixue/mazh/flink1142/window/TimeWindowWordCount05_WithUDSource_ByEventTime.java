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
 *  注释： 需求：每隔 5秒 计算最近 10秒 的单词次数
 *  重点： 处理乱序
 *  -
 *  程序的要点：
 *  1、数据源依然发送轮序数据流：13,16,19
 *  2、要通过 assignTimestampsAndWatermarks() 来指定 eventTime 的定义
 *  3、计算逻辑没有变
 */
public class TimeWindowWordCount05_WithUDSource_ByEventTime {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.addSource(new TestSource());

        // TODO_MA 马中华 注释： 输入数据： "flink," + System.currentTimeMillis();
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] fields = s.split(",");
                        //拆分字段   Tuple2(flink, timestamp)
                        collector.collect(Tuple2.of(fields[0], Long.valueOf(fields[1])));
                    }
                })
                // TODO_MA 马中华 注释： 要去指定到底使用 event 中什么信息什么字段当做这条 event 的 eventTime 呢
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                        .withTimestampAssigner((ctx) -> new TimeStampExtractor())) //3)指定时间字段
                .keyBy(tuple -> tuple.f0) //指定分组的字段
                //1. 指定了时间的类型。
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //滑动窗口
                .process(new SumProcessFunction()) //计算结果
                .print();

        // TODO_MA 马中华 注释：
        env.execute("TimeWindowWordCount05_WithUDSource_ByEventTime");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 指定时间字段
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
            System.out.println(eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 指定 eventTime 字段
     */
    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            // TODO_MA 马中华 注释： Tuple2<String, Long> element
            // TODO_MA 马中华 注释：  Tuple2<String, Long> = flink,16723048092
            //这个地方的时间单位是毫秒
            return element.f1;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 输出的日志中，带有 EventTime 了
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

            // TODO_MA 马中华 注释： 13s 输出一条数据
            TimeUnit.SECONDS.sleep(13);
            // 日志里面带有事件时间
            String event = "flink," + System.currentTimeMillis();
            String event1 = event;
            cxt.collect(event);

            // TODO_MA 马中华 注释： 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("flink," + System.currentTimeMillis());

            // TODO_MA 马中华 注释： 本该 13s 输出的一条数据，延迟到 19s 的时候才输出
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event1);

            TimeUnit.SECONDS.sleep(300000);
        }

        @Override
        public void cancel() {
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
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
