package org.naixue.mazh.flink1142.window2.lesson06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 使用 Evictor 自己实现一个类似 CountWindow(3,2) 的效果
 *  每隔 2个 单词计算最近 3个 单词
 */
public class FlinkWindowWordCount_ByUDEvictor {

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

        // TODO_MA 马中华 注释： 每隔 2个 单词，统计最近 三个单词的出现次数
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> keyedWindow = stream
                .keyBy(tuple -> tuple.f0)
                // TODO_MA 马中华 注释： 指定窗口的生成规则
                .window(GlobalWindows.create())
                // TODO_MA 马中华 注释： 指定触发计算的规则，MyCountTrigger 维护每接收到两个单词，就触发一次计算
                .trigger(new MyCountTrigger(2))
                // TODO_MA 马中华 注释： 指定从窗口淘汰数据的规则， MyCountEvictor 维护这个窗口中，只有 3 个数据
                .evictor(new MyCountEvictor(3));

        // TODO_MA 马中华 注释： this.window(GlobalWindows.create())
        // TODO_MA 马中华 注释： .evictor(CountEvictor.of(size))
        // TODO_MA 马中华 注释： .trigger(CountTrigger.of(slide));
        // stream.keyBy(tuple -> tuple.f0).countWindow(3, 2);

        // TODO_MA 马中华 注释：
        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        // TODO_MA 马中华 注释：
        wordCounts.print().setParallelism(1);

        // TODO_MA 马中华 注释：
        env.execute("FlinkWindowWordCount_ByUDEvictor");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 Trigger
     */
    private static class MyCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {
        // 表示指定的元素的最大的数量
        private long maxCount;

        // 用于存储每个 key 对应的 count 值
        private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>("count",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long aLong, Long t1) throws Exception {
                        return aLong + t1;
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
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window,
                                       TriggerContext ctx) throws Exception {
            // 拿到当前 key 对应的 count 状态值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            // count 累加 1
            count.add(1L);
            // 如果当前 key 的 count 值等于 maxCount
            if (count.get() == maxCount) {
                count.clear();
                // 触发 window 计算
                return TriggerResult.FIRE;
            }
            // 否则，对 window 不做任何的处理
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 写基于 Processing Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

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

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 Evictor
     *  泛型1： 窗口的输入数据的类型；Tuple2<String, Integer>
     *  泛型2： 窗口的类型
     *  假设现在 window 已经有 3 个元素了。 现在来2个元素，窗口里面有 5 个。
     *  但是我们只需要获取最近的 3 个来执行计算，所以需要 evict 一部分数据（最先进入 window 的数据）出去
     */
    private static class MyCountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {
        // window 的大小
        private long windowCount;

        public MyCountEvictor(long windowCount) {
            this.windowCount = windowCount;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 在 window 计算之前删除特定的数据
         *  @param elements  window 中所有的元素
         *  @param size  window 中所有元素的大小
         *  @param window  window
         *  @param evictorContext  上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window,
                                EvictorContext evictorContext) {
            // 如果当前的总的数据小于窗口的大小，就不需要删除数据了
            if (size <= windowCount) {
                return;
            } else {
                // 需要删除数据
                // 需要删除的数据的个数
                int evictorCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                // TODO_MA 马中华 注释： 迭代
                while (iterator.hasNext()) {
                    iterator.next();
                    evictorCount++;
                    // TODO_MA 马中华 注释： 满足这个条件，其实就是表示，窗口的数据条数，依然大于 windowCount
                    if (size - windowCount >= evictorCount) {
                        // 删除了元素
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                // TODO_MA 马中华 注释： 1 2 3 4 5
                // TODO_MA 马中华 注释： 淘汰：1 2 ， 留下来 3 4 5
            }
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 在 window 计算之后删除特定的数据
         *  @param elements  window 中所有的元素
         *  @param size  window 中所有元素的大小
         *  @param window   window
         *  @param evictorContext  上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window,
                               EvictorContext evictorContext) {
        }
    }
}