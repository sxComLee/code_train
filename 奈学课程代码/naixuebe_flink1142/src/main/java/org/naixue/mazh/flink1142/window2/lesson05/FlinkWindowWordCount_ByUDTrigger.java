package org.naixue.mazh.flink1142.window2.lesson05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：使用 Trigger 自己实现一个类似 CountWindow 的效果
 *  具体实现： countWindow(3)
 *  1、这个单词出现了 3 次就输出一次。wordcount 需求下，能看到这样的效果
 *  2、这个 key 的 window 内部有三个元素了，就触发计算，做输出
 *  -
 *  通过自定义 Trigger 来实现！
 */
public class FlinkWindowWordCount_ByUDTrigger {

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

        // TODO_MA 马中华 注释： 调用自定义的 Trigger 来完成触发
        stream.keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create()) //global window
                // TODO_MA 马中华 注释： 自定义一个 Trigger
                .trigger(new MyCountTrigger(3))
                .sum(1)
                .print();

        // TODO_MA 马中华 注释： 可以看看里面的源码，跟我们写的很像
        /*WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow = stream.keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3));
        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);
        wordCounts.print()
                .setParallelism(1);*/

        // TODO_MA 马中华 注释：
        env.execute("FlinkWindowWordCount_ByUDTrigger");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义触发器: 每三个元素计算一次
     *  Tuple2<String, Integer> 输入的数据类型
     *  GlobalWindow  窗口的数据类型
     *  有 4 个抽象方法，需要去实现： 其中 onElement 表示每次接收到这个 window 的一个输入，就调用一次
     */
    private static class MyCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {

        // 表示指定的元素的最大的数量
        private long maxCount;

        // 用于存储每个 key 对应的 count 值
        // 当遇到复杂需求的时候，一般都是使用 mapstate 和 liststate
        private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>("count",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }, Long.class
        );

        public MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 当一个元素进入到一个 window 中的时候就会调用这个方法
         * @param element   元素
         * @param timestamp 进来的时间
         * @param window    元素所属的窗口
         * @param ctx 上下文
         * @return TriggerResult
         *      1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         *      2. TriggerResult.FIRE ：表示触发 window 的计算
         *      3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         *      4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除 window 中的数据
         * Tuple2<String, Integer> element = <a,1>
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window,
                                       TriggerContext ctx) throws Exception {
            // 拿到当前 key 对应的 count 状态值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);

            // count 累加 1
            count.add(1L);

            // 如果当前 key 的 count 值等于 maxCount
            // 单词出现了三次才输出
            // 接收到三个输入才输出
            if (count.get() == maxCount) {
                // ReducingState 清空
                count.clear();
                // 触发 window 计算，删除数据
                // 清空整个窗口的数据
                return TriggerResult.FIRE_AND_PURGE;
            }else{
                // 否则，对 window 不做任何的处理
                return TriggerResult.CONTINUE;
            }
        }

        // TODO_MA 马中华 注释： 我们一般也不用定时器，所以这两个方法里面是没有逻辑的
        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 写基于 Processing Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        // TODO_MA 马中华 注释： 我们一般也不用定时器，所以这两个方法里面是没有逻辑的
        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 写基于 Event Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            // 清除状态值
            ctx.getPartitionedState(stateDescriptor).clear();
        }
    }
}
