package org.naixue.mazh.flink1142.window2.lesson07;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 演示增量聚合
 */
public class FlinkSocket_AggregateFunction_Incremental {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： intDStream 是一个 元素为 Int 类型的数据流
         *  没有做 keyBy()  所以调用 windowAll 来进行窗口创作
         */
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.windowAll(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // TODO_MA 马中华 注释： 来定义聚合逻辑
        // TODO_MA 马中华 注释： ReduceFunction 的 泛型，就是数据流的 元素的类型
        // TODO_MA 马中华 注释： 这个操作实现增量聚合： 窗口中每次进入一个元素，就调用 reduce 执行一次聚合
        windowResult.reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer last, Integer current) throws Exception {
                        System.out.println("执行逻辑" + last + "  " + current);
                        // TODO_MA 马中华 注释： 求和
                        return last + current;
                    }
                })
                .print();

//        windowResult.sum().print()

        // TODO_MA 马中华 注释：
        env.execute(FlinkSocket_AggregateFunction_Incremental.class.getSimpleName());
    }
}
