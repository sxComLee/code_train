package org.naixue.mazh.flink1142.window2.lesson07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求： 求每隔窗口中的数据的平均值
 *  思路： 累加值/总的个数=平均值
 */
public class FlinkWindow_Aggregate_WindowAvg {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Integer> numberStream = dataStream.map(line -> Integer.valueOf(line));
        AllWindowedStream<Integer, TimeWindow> windowStream = numberStream.windowAll(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // TODO_MA 马中华 注释： 调用自定义 AggregateFunction = MyAggregate
        windowStream.aggregate(new MyAggregate())
                .print();

        // TODO_MA 马中华 注释：
        env.execute("FlinkWindow_Aggregate");
    }

    // TODO_MA 马中华 注释： 只要你们发现实现某个事儿有两种方式的时候， map  flatMap    reduce  aggregate
    // TODO_MA 马中华 注释： aggregate 规范
    // TODO_MA 马中华 注释： reduce 就是常用实现

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义 AggregateFunction
     *  IN, 输入的数据类型
     *  ACC,自定义的中间状态 Tuple2<Integer,Integer>: key: 计算数据的个数 + value:计算总值
     *  OUT，输出的数据类型
     */
    private static class MyAggregate implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {

        // TODO_MA 马中华 注释： 初始化 累加器，辅助变量
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            // key 累积有多少个数
            // value 累积总的值
            return new Tuple2<>(0, 0);
        }

        // TODO_MA 马中华 注释： 针对每个数据的操作
        // TODO_MA 马中华 注释： 聚合： 临时结果 + 一条数据
        @Override
        public Tuple2<Integer, Integer> add(Integer element, Tuple2<Integer, Integer> accumulator) {
            // 个数+1
            // 总的值累计
            return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + element);
        }

        // TODO_MA 马中华 注释： 针对多个临时结果的合并操作
        // TODO_MA 马中华 注释： 两个临时结果的 合并
        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a1, Tuple2<Integer, Integer> b1) {
            return Tuple2.of(a1.f0 + b1.f0, a1.f1 + b1.f1);
        }

        // TODO_MA 马中华 注释： 获取最终计算值
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return (double) accumulator.f1 / accumulator.f0;
        }
    }
}
