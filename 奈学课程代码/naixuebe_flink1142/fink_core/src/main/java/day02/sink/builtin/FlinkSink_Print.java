package day02.sink.builtin;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： print sink 测试
 */
public class FlinkSink_Print {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<Tuple3<Integer, String, Double>> sourceDS = executionEnvironment.fromElements(
                Tuple3.of(19, "xuzheng", 178.8), Tuple3.of(17, "huangbo", 168.8), Tuple3.of(18, "wangbaoqiang", 174.8),
                Tuple3.of(18, "liujing", 195.8), Tuple3.of(18, "liutao", 182.7), Tuple3.of(21, "huangxiaoming", 184.8)
        );

        // TODO_MA 马中华 注释：
        sourceDS.print().setParallelism(1);

        // TODO_MA 马中华 注释：
        sourceDS.printToErr().setParallelism(1);

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("FlinkSink_Print");
    }
}
