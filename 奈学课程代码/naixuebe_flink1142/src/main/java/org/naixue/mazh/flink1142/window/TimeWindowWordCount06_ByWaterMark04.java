package org.naixue.mazh.flink1142.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  需求：3秒一个窗口，把相同 key合并起来
 *  -
 *  flink,1461756870000
 *  flink,1461756883000
 *  迟到的数据
 *  flink,1461756870000
 *  flink,1461756871000
 *  flink,1461756872000
 */
public class TimeWindowWordCount06_ByWaterMark04 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-date") {
        };

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String line) throws Exception {
                        String[] fields = line.split(",");
                        return new Tuple2<>(fields[0], Long.valueOf(fields[1]));
                    }
                    //步骤二：获取数据里面的event Time
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                        .withTimestampAssigner((ctx) -> new TimeStampExtractor())) //指定时间字段
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //.timeWindow(Time.seconds(3))
                //.allowedLateness(Time.seconds(2)) // 允许事件迟到 2 秒
                .sideOutputLateData(outputTag) //保留迟到太多的数据
                .process(new SumProcessWindowFunction());

        // TODO_MA 马中华 注释： 输出结果
        result.print();


        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理延迟的数据： 侧输出流
         *  都是那些过分延迟到达的数据
         */
        result.getSideOutput(outputTag)
                .map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        /**
                         * 这个地方，我这儿只是把打印出来，实际工作里面：
                         * 可以把存储到持久化的引擎里面：1）kafka topic  2) MySQL 3) redis
                         * 1. 如果迟到的数据确实多，我们得调试程序。
                         * 2. 如果迟到的数据确实不多，我们就可以忽略不计。
                         */
                        return "迟到数据：" + stringLongTuple2.toString();
                    }
                })
                .print();

        // TODO_MA 马中华 注释： 提交 job 执行
        env.execute("WindowWordCountByWaterMark2");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  IN, OUT, KEY, W
     *  IN：输入的数据类型
     *  OUT：输出的数据类型
     *  Key：key的数据类型（在Flink里面，String用Tuple表示）
     *  W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) throws Exception {

            System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time : " + dateFormat.format(context.window()
                    .getStart()));

            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());

            System.out.println("window end time  : " + dateFormat.format(context.window()
                    .getEnd()));
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 watermark 生成器
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        private long currentMaxEventTime = 0L;
        // 最大允许的乱序时间 10 秒
        private long maxOutOfOrderness = 10000;

        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {

            // TODO_MA 马中华 注释： 记录更新 窗口内的 最大 eventTime
            long currentElementEventTime = event.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

            System.out.println("event = " + event
                    // Event Time
                    + " | " + dateFormat.format(event.f1)
                    // Max Event Time
                    + " | " + dateFormat.format(currentMaxEventTime)
                    // Current Watermark
                    + " | " + dateFormat.format(currentMaxEventTime - maxOutOfOrderness));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 指定窗口内数据的 记录中的 EventTime 字段
     */
    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }
}
