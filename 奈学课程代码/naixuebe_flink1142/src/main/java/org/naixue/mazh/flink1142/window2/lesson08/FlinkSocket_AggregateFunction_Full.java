package org.naixue.mazh.flink1142.window2.lesson08;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 全量计算
 */
public class FlinkSocket_AggregateFunction_Full {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));

        // TODO_MA 马中华 注释： 需求： 每隔 5s 执行一次计算
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(5));

        // TODO_MA 马中华 注释： ProcessAllWindowFunction 全量聚合
        // TODO_MA 马中华 注释： ProcessAllWindowFunction 的匿名对象
        // TODO_MA 马中华 注释： 当一个窗口结束的时候要出发计算，就会调用 这个匿名对象的 process 方法处理一次
        windowResult.process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {

                    // TODO_MA 马中华 注释： iterable 就是这个窗口中的所有数据形成的一个迭代器
                    @Override
                    public void process(Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                        System.out.println("执行计算逻辑");
                        // TODO_MA 马中华 注释： 求和， 全量求和
                        int count = 0;
                        Iterator<Integer> numberiterator = iterable.iterator();
                        while (numberiterator.hasNext()) {
                            Integer number = numberiterator.next();
                            count += number;
                        }
                        collector.collect(count);
                    }
                })
                .print();

        // TODO_MA 马中华 注释： FlinkSocket_AggregateFunction_Full
        env.execute("FlinkSocket_AggregateFunction_Full");
    }
}
