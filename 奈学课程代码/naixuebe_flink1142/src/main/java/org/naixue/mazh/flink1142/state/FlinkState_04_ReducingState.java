package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求： 自定义累积求和
 *  1、原始的 wordcount 程序中，使用 的 sum 自带状态管理
 *  2、不使用 sum 算子来完成 sum 的逻辑处理
 */
public class FlinkState_04_ReducingState {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple2<Long, Long>> sourceDS = environment.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 5L), Tuple2.of(2L, 4L), Tuple2.of(2L, 3L), Tuple2.of(2L, 5L)
        );

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Tuple2<Long, Long>> resultDS = sourceDS.keyBy(0)
                // TODO_MA 马中华 注释： 下面这句代码的作用，就是实现和 sum 一样的效果
                .flatMap(new SumByReducingStateFunction());

        // TODO_MA 马中华 注释：
        resultDS.print();

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_ReducingState");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义的用来实现累积求和效果的聚合函数
     *  oldState + input = newState
     */
    static class SumByReducingStateFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private ReducingState<Long> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Long> reduceStateDesc = new ReducingStateDescriptor<>("reduce_state",
                    // TODO_MA 马中华 注释： 在初始化 ReducingState 定义了一个聚合函数
                    new ReduceFunction<Long>() {
                        // TODO_MA 马中华 注释： oldState + input = newState
                        @Override
                        public Long reduce(Long num1, Long num2) throws Exception {
                            return num1 + num2;
                        }
                    }, Long.class
            );
            reducingState = getRuntimeContext().getReducingState(reduceStateDesc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, Long>> collector) throws Exception {

            // TODO_MA 马中华 注释： 将输入，合并到 reducingState 中
            // TODO_MA 马中华 注释： 会自动执行： oldState + input = newState
            reducingState.add(record.f1);

            // TODO_MA 马中华 注释： 然后输出 reduceState 当中的值
            collector.collect(Tuple2.of(record.f0, reducingState.get()));
        }
    }
}
