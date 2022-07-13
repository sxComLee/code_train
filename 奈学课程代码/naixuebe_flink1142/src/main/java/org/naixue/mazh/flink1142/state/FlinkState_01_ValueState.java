package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求：每 3 个相同 key 就输出他们的 平均值
 *  输入数据：
 *  Tuple2.of(1L, 3L),
 *  Tuple2.of(1L, 7L),
 *  Tuple2.of(2L, 4L),
 *  Tuple2.of(1L, 5L),
 *  Tuple2.of(2L, 3L),
 *  Tuple2.of(2L, 5L)
 *  输出：
 *  (1,5.0)
 *  (2,4.0)
 *  -
 *  两个重点：
 *  1、一个 key 一个 valuestate
 *  2、这个代码中的，任何地方的泛型都不是固定的，都是跟着你的需求来走
 */
public class FlinkState_01_ValueState {

    // TODO_MA 马中华 注释： Flink 的流式编程，标准的五步骤
    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释： 需求： 每 3 个相同的 key 输出他们的平均值
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
        SingleOutputStreamOperator<Tuple2<Long, Double>> resultDS = sourceDS.keyBy(0)
                // TODO_MA 马中华 注释： 状态编程
                // TODO_MA 马中华 注释： flatmap 参数，就必然是一个 FlatMapFunction 的实现类的实例
                // TODO_MA 马中华 注释： 具体使用的其实是： RichFlatMapFunction
                // TODO_MA 马中华 注释： 在 flatmap 函数的内部，一定会调用 匿名参数实例的 flatMap 方法
                .flatMap(new CountAverageWithValueState());
        // TODO_MA 马中华 注释： map 和 flatmap 最大的区别是什么？
        // TODO_MA 马中华 注释： 1、map(f) 中的 函数f，接受一个输入，必须输出一个值
        // TODO_MA 马中华 注释： 2、flatmap(f) 中的参数函数f，接收一个值，可以输出 0,1,N
        // TODO_MA 马中华 注释： 如果仅仅只是简单的映射操作，用map   Integer ==> String

        // TODO_MA 马中华 注释： Sink 操作
        resultDS.print();

        // TODO_MA 马中华 注释： 提交
        environment.execute("FlinkState_ValueState");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： Value State 自定义实现
     *  RichFlatMapFunction<Input, Output>
     *  Source 自定义： implements SourceFunction  &  extends RichSourceFunction
     *  Sink 自定义： implements SinkFunction  &  extends RichSinkFunction
     *  -
     *  RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> 的第一个泛型参数： 输入数据的类型
     */
    static class CountAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        // TODO_MA 马中华 注释： Value state 成员变量
        // TODO_MA 马中华 注释： valuestate 的泛型代表的意思是： 某个key出现的次数和总和
        // TODO_MA 马中华 注释： 一个 key 就有一个这样的 valueState
        private ValueState<Tuple2<Long, Long>> countAndSumState;
        // TODO_MA 马中华 注释： Integer， String， Tuple，  Student

        // TODO_MA 马中华 注释： 初始化方法
        @Override
        public void open(Configuration parameters) throws Exception {
            // TODO_MA 马中华 注释： 先声明一个 XXXStateDescriptor 用来描述状态的相关信息
            ValueStateDescriptor<Tuple2<Long, Long>> countAndSumStateDesc = new ValueStateDescriptor<>("countAndSumState",
                    Types.TUPLE(Types.LONG, Types.LONG)
            );
            countAndSumState = getRuntimeContext().getState(countAndSumStateDesc);
        }

        // TODO_MA 马中华 注释： 状态处理 和 逻辑处理
        // TODO_MA 马中华 注释： 输入； Tuple2<Long, Long> record,
        // TODO_MA 马中华 注释： 方法没有返回值，但是可以决定是否输出结果， 最终是否要输出结果 通过 collector 来输出
        // TODO_MA 马中华 注释： 每 3 个相同的 key 输出他的 平均值
        // TODO_MA 马中华 注释： 主要的两点：
        // TODO_MA 马中华 注释： 1、接收到这个 key 的前两条数据的时候，需要做状态更新，不做计算
        // TODO_MA 马中华 注释： 2、当接收到第三条数据的时候，从状态中，获取出来之前存储的两条元素，计算平均值，通过 collector 输出这个计算结果
        // TODO_MA 马中华 注释： 一个 key 一个 ValueState， 这个代码时候在分布式环境中
        @Override
        public void flatMap(Tuple2<Long, Long> record, Collector<Tuple2<Long, Double>> collector) throws Exception {
            // 如果当前这个 key 的 ValueState 不存在，则初始化一个
            Tuple2<Long, Long> currentState = countAndSumState.value();
            if (currentState == null){
                currentState = Tuple2.of(0L, 0L);
            }
            // 状态处理（key 的出现次数+1， key的值的总和做累加）
            currentState.f0 += 1;
            currentState.f1 += record.f1;

            // 状态更新
            countAndSumState.update(currentState);

            // TODO_MA 马中华 注释： 满足输出要求了，
            if (currentState.f0 == 3){
                // TODO_MA 马中华 注释： 执行计算，得到计算结果
                double avg = (double) currentState.f1 / currentState.f0;
                // TODO_MA 马中华 注释： 输出
                collector.collect(Tuple2.of(record.f0, avg));
                // TODO_MA 马中华 注释： 状态清空
                countAndSumState.clear();
            }else{

            }
        }
    }
}
