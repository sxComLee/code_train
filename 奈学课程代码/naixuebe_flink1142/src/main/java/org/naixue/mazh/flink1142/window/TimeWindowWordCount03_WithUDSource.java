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
 *  注释： 需求：每隔5秒计算最近10秒的单词次数
 */
public class TimeWindowWordCount03_WithUDSource {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                .print();

        // TODO_MA 马中华 注释：
        env.execute("WindowWordCountAndTime");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义的 source
     *  1、在第 13s 的时候，输出两条数据
     *  2、在第 16s 的时候，输出一条数据
     */
    public static class TestSource implements SourceFunction<String> {

        // TODO_MA 马中华 注释： 时间格式化器
        FastDateFormat dateformat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> cxt) throws Exception {

            // TODO_MA 马中华 注释： 20:53:02 - 20:53:10
            String currTime = String.valueOf(System.currentTimeMillis());

            // TODO_MA 马中华 注释： 这个操作是我为了保证是 10s 的倍数。
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }

            // TODO_MA 马中华 注释： 20:53:10 ， 20:53:20  20:53:30  20:53:40
            // TODO_MA 马中华 注释： 此时： 20:53:10
            System.out.println("当前时间：" + dateformat.format(System.currentTimeMillis()));

            // TODO_MA 马中华 注释： 13s 输出两条数据 ：  20:53:23
            TimeUnit.SECONDS.sleep(13);
            cxt.collect("flink");
            cxt.collect("flink");

            // TODO_MA 马中华 注释： 16s 输出一条数据  :  20:53:26
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("flink");

            TimeUnit.SECONDS.sleep(30000000);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 最综的效果：
         *  20:53:10  不触发 window 计算
         *  20:53:15  不触发 window 计算
         *  20:53:20  不触发 window 计算
         *  20:53:25  [20:53:15 - 20:53:25]   flink,2
         *  20:53:30  [20:53:20 - 20:53:30]   flink,3
         *  20:53:35  [20:53:25 - 20:53:35]   flink,1
         *  20:53:40  不触发 window 计算
         */

        @Override
        public void cancel() {
        }
    }

    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int count = 0;
            for (Tuple2<String, Integer> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }
}
