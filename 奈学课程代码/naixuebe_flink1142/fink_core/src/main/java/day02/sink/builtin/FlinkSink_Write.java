package day02.sink.builtin;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 将生成的 数据流 中的数据，写到文件
 */
public class FlinkSink_Write {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        // TODO_MA 马中华 注释：
        ArrayList<String> data = new ArrayList<>();
        data.add("huangbo");
        data.add("xuzheng");
        data.add("wangbaoqiang");
        data.add("shenteng");

        // TODO_MA 马中华 注释：
        DataStreamSource<String> dataSourceDS = executionEnvironment.fromCollection(data);

        // TODO_MA 马中华 注释： 把结果输出到文件系统！
        dataSourceDS.writeAsText("file:///C:\\bigdata-data\\flink_sink\\resultfile.txt")
                .setParallelism(1);

        executionEnvironment.execute("FlinkSink_Write");
    }
}
