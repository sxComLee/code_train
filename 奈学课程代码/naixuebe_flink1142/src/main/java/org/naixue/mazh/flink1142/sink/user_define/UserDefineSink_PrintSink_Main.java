package org.naixue.mazh.flink1142.sink.user_define;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 */
public class UserDefineSink_PrintSink_Main {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境对象
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 构造数据
        DataStreamSource<Tuple3<Integer, String, Double>> sourceDS = executionEnvironment.fromElements(
                Tuple3.of(19, "xuzheng", 178.8), Tuple3.of(17, "huangbo", 168.8), Tuple3.of(18, "wangbaoqiang", 174.8),
                Tuple3.of(18, "liujing", 195.8), Tuple3.of(18, "liutao", 182.7), Tuple3.of(21, "huangxiaoming", 184.8)
        );

        // TODO_MA 马中华 注释： 第一种实现
        sourceDS.addSink(new SinkFunction<Tuple3<Integer, String, Double>>() {
            @Override
            public void invoke(Tuple3<Integer, String, Double> value) throws Exception {
                System.out.println("第一种实现：" + value);
            }
        }).setParallelism(1);

        // TODO_MA 马中华 注释： 第二种实现
        sourceDS.addSink(new RichSinkFunction<Tuple3<Integer, String, Double>>() {
            @Override
            public void invoke(Tuple3<Integer, String, Double> value) throws Exception {
                System.out.println("第二种实现：" + value);
                // TODO_MA 马中华 注释： 直接打印输出
                // TODO_MA 马中华 注释： 写出到 MySQL
                // TODO_MA 马中华 注释： 写出 ES
            }
        });

        // TODO_MA 马中华 注释： 提交执行
        executionEnvironment.execute("UserDefineSink_PrintSink_Main");
    }
}