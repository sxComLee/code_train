package org.naixue.mazh.flink1142.window2.lesson08;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class FlinkWindowJoin {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Tuple2<String, String>> data1 = dataStream.map(
                new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String word) throws Exception {
                        return Tuple2.of(word, word);
                    }
                });

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataStream2 = env.socketTextStream("bigdata02", 3456);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Tuple2<String, String>> data2 = dataStream2.map(
                new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String word) throws Exception {
                        return Tuple2.of(word, word);
                    }
                });

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Join 实现
         */
        data1.join(data2)
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple2<String, String> t2) throws Exception {
                        System.out.println("t1 " + t1.toString() + "t2 " + t2.toString());
                        return t1.toString() + t2.toString();
                    }
                })
                .print();

        // TODO_MA 马中华 注释：
        env.execute("FlinkWindowJoin");
    }
}
