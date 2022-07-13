package org.naixue.mazh.flink1142.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每隔 5秒 计算最近 10秒 的单词次数
 *  如果数据乱序了，那么基于 processingtime 处理有什么后果呢？
 */
public class TimeWindowWordCount04_WithUDSource2 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.addSource(new TestSource());

        // TODO_MA 马中华 注释：
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] fields = line.split(",");
                        for (String word : fields) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessFunction())
                .print()
                .setParallelism(1);

        // TODO_MA 马中华 注释：
        env.execute("WindowWordCountBySource2");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义乱序数据源
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
            String log = "flink";
            cxt.collect(log);

            // TODO_MA 马中华 注释： 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("flink");

            // TODO_MA 马中华 注释： 本该 13s 输出的一条数据延迟到 19s 的时候才输出
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(log);

            TimeUnit.SECONDS.sleep(300000);
        }

        @Override
        public void cancel() {
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 注意 ProcessWindowFunction 抽象类的四个泛型
     *  IN 输入数据类型
     *  OUT 输出数据类型
     *  KEY key
     *  W extends Window window类型
     */
    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            // TODO_MA 马中华 注释： 求和
            int count = 0;
            for (Tuple2<String, Integer> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }
}
