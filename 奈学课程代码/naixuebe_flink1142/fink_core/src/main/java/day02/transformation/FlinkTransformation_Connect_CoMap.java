package day02.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 和 union 类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 */
public class FlinkTransformation_Connect_CoMap {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 生成两个数据流
        // TODO_MA 马中华 注释： text1 = (1,2,3,4,5,6)
        DataStreamSource<Long> text1 = executionEnvironment.addSource(new UserDefineSource_NoParallel()).setParallelism(1);

        // TODO_MA 马中华 注释：
        // TODO_MA 马中华 注释： strDS = (str_1, str_2, str_3)
        DataStreamSource<Long> text2 = executionEnvironment.addSource(new UserDefineSource_NoParallel()).setParallelism(1);
        SingleOutputStreamOperator<String> strDS = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        // TODO_MA 马中华 注释： connect 链接： 结果数据流的 数据类型是： 元组（第一个流的泛型,  第二个流的泛型）
        // TODO_MA 马中华 注释： zip
        ConnectedStreams<Long, String> connectedStreams = text1.connect(strDS);

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<Object> comapResultDS = connectedStreams.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value * 2;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value + "_" + value;
            }
        });

        // TODO_MA 马中华 注释：
        comapResultDS.print().setParallelism(1);

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("FlinkTransformation_Connect_CoMap");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 自定义一个不带并行度的 Source
     */
    public static class UserDefineSource_NoParallel implements SourceFunction<Long> {

        private boolean isRunning = true;

        private long number = 1L;

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning) {
                sourceContext.collect(number++);
                // TODO_MA 马中华 注释： 每隔 1s 钟发送一条数据
//                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
