package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求： 不断输出每个 key 的 value 列表
 */
public class FlinkState_05_AggregatingState {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple2<Long, Long>> sourceDS = environment.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 3L),
                Tuple2.of(2L, 5L)
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 使用三种 State 来计算
         *  1、ValueState 只会存储一个单一的值，注意，一个 Key 一个 ValueState
         *  2、ListState 存储了数据列表，注意，一个 Key 一个 ListState
         *  3、MapState 存储数据集合，注意，一个 Key 一个 MapState
         */
        SingleOutputStreamOperator<Tuple2<Long, String>> resultDS = sourceDS.keyBy(0)
                .flatMap(new ContainsValueAggregatingState());

        // TODO_MA 马中华 注释：
        resultDS.print();

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_AggregatingState");
    }

    static class ContainsValueAggregatingState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

        // TODO_MA 马中华 注释： 状态
        private AggregatingState<Long, String> aggState;

        @Override
        public void open(Configuration parameters) throws Exception {

            // TODO_MA 马中华 注释：
            AggregatingStateDescriptor aggStateDesc = new AggregatingStateDescriptor<>("agg_state",

                    // TODO_MA 马中华 注释： 自定义一个聚合逻辑
                    // TODO_MA 马中华 注释： AggregateFunction 的 add 方法： 分区内的计算
                    // TODO_MA 马中华 注释： AggregateFunction 的 merge 方法： 分区之间的计算
                    // TODO_MA 马中华 注释： hive 的 UDAF 怎么定义！
                    new AggregateFunction<Long, String, String>() {

                        // TODO_MA 马中华 注释： 创建一个初始变量
                        @Override
                        public String createAccumulator() {
                            return "值列表：";
                        }

                        // TODO_MA 马中华 注释： 进行元素的 agg 累积
                        // TODO_MA 马中华 注释： 假设依次接收到的值分别是 1 2 3
                        // TODO_MA 马中华 注释： 值列表：1
                        // TODO_MA 马中华 注释： 值列表：1,2
                        // TODO_MA 马中华 注释： 值列表：1,2,3
                        // TODO_MA 马中华 注释： String 字符串的拼接
                        @Override
                        public String add(Long value, String accumulator) {
                            if (accumulator.equals("值列表：")) {
                                return accumulator + value;
                            } else {
                                return accumulator + "," + value;
                            }
                        }

                        // TODO_MA 马中华 注释： 获取最终结果
                        @Override
                        public String getResult(String s) {
                            return s;
                        }

                        // TODO_MA 马中华 注释： 分区合并
                        @Override
                        public String merge(String s, String acc1) {
                            return s + "," + acc1;
                        }
                    }, String.class
            );

            // TODO_MA 马中华 注释： 获取状态
            aggState = getRuntimeContext().getAggregatingState(aggStateDesc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, String>> collector) throws Exception {
            aggState.add(record.f1);
            collector.collect(Tuple2.of(record.f0, aggState.get()));
        }
    }
}
