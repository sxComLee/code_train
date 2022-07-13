package org.naixue.mazh.flink1142.window2.lesson03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 5秒 没有单词输出，则输出该单词的单词次数
 *  通过 state 编程实现 session window 的效果
 */
public class FlinkWordCount_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置每个 operator 的并行度
        env.setParallelism(1);
        // socket 数据源不是一个可以并行的数据源

        // TODO_MA 马中华 注释：
        env.socketTextStream("bigdata02", 6789)
                .flatMap(new WordOneFlatMapFunction())
                .keyBy(tuple -> tuple.f0)
                // TODO_MA 马中华 注释： CountWithTimeoutFunction的作用，其实是内部维护了每一个 key 的一个 ValueState
                // TODO_MA 马中华 注释： ValueState 中的必须维护了这个 key 的最后一次到达时间
                // TODO_MA 马中华 注释： 每次接收到这个 key 的一条数据的时候，就会重新注册一个定时器
                // TODO_MA 马中华 注释： 如果这个定时器没有被重新注册，并且定时时间到来的话，就会触发一次计算
                // TODO_MA 马中华 注释： 触发计算，就是对 valueState 执行一个逻辑处理
                .process(new CountWithTimeoutFunction())
                .print();

        // TODO_MA 马中华 注释： 5. 启动并执行流程序
        env.execute("FlinkWordCount_KeyedProcessFunction");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 Function 来完成类似于 窗口的 计算效果
     */
    private static class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        // TODO_MA 马中华 注释： 通过 valuestate 来累计记录条数
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState", CountWithTimestamp.class));
        }

        // TODO_MA 马中华 注释： 该方法，每次接收到一条数据，就会执行一次
        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx,
                                   Collector<Tuple2<String, Integer>> collector) throws Exception {
            // TODO_MA 马中华 注释： 拿到当前 key 的对应的状态
            CountWithTimestamp currentState = state.value();
            if (currentState == null) {
                currentState = new CountWithTimestamp();
                currentState.key = element.f0;
            }

            // TODO_MA 马中华 注释： 更新这个 key 出现的次数
            currentState.count++;

            // TODO_MA 马中华 注释： 更新这个 key 到达的时间，最后修改这个状态时间为当前的 Processing Time
            currentState.lastModified = ctx.timerService().currentProcessingTime();

            // TODO_MA 马中华 注释： 更新状态
            state.update(currentState);

            // TODO_MA 马中华 注释： 注册一个定时器： 注册一个以 Processing Time 为准的定时器
            // TODO_MA 马中华 注释： 如果触发了定时器任务的话，就会调用 onTimer 方法来触发计算
            // 定时器触发的时间是当前 key 的最后修改时间加上 5 秒
            ctx.timerService().registerProcessingTimeTimer(currentState.lastModified + 5000);
            // TODO_MA 马中华 注释： 每个key维护了一个 valuestate，同时也维护一个 timer，如果重新注册，就覆盖了之前的
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 定时器需要运行的逻辑
         *  timestamp = 定时器触发的时间戳
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            // TODO_MA 马中华 注释： 先拿到当前 key 的状态
            CountWithTimestamp curr = state.value();

            // TODO_MA 马中华 注释： 检查这个 key 是不是 5 秒钟没有接收到数据
            if (timestamp == curr.lastModified + 5000) {

                // TODO_MA 马中华 注释： 输出计算结果
                out.collect(Tuple2.of(curr.key, curr.count));

                // TODO_MA 马中华 注释： 清空 state
                state.clear();
            }
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 存储的数据
     */
    private static class CountWithTimestamp {
        // TODO_MA 马中华 注释： 单词
        public String key;
        // TODO_MA 马中华 注释： 次数
        public int count;
        // TODO_MA 马中华 注释： 最后计算/处理的时间
        public long lastModified;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 FlatMapFunction
     */
    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.toLowerCase().split(",");
            for (String word : words) {
                Tuple2<String, Integer> wordOne = new Tuple2<>(word, 1);
                // 将单词计数 1 的二元组输出
                out.collect(wordOne);
            }
        }
    }
}

