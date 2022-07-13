package day02.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Map_Filter 算子测试
 */
public class FlinkTransformation_Map_Filter {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        // TODO_MA 马中华 注释：
        ArrayList<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(2);
        data.add(3);
        data.add(4);

        // TODO_MA 马中华 注释：
        DataStreamSource<Integer> dataDS = executionEnvironment.fromCollection(data);

        // TODO_MA 马中华 注释： dataDS.map(f)   mf = new MapFunction();   f = mf.map(Integer value)
        SingleOutputStreamOperator<Integer> dataStream = dataDS.map(new MapFunction<Integer, Integer>() {
            // TODO_MA 马中华 注释： value 方法的参数 输入
            // TODO_MA 马中华 注释： Integer 方法的返回值的，输出
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("接受到了数据：" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Integer> filterDS = dataStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer number) throws Exception {
                return number % 2 == 0;
            }
        });

        filterDS.print().setParallelism(1);

        executionEnvironment.execute("FlinkTransformation_Map_Filter");
    }
}
