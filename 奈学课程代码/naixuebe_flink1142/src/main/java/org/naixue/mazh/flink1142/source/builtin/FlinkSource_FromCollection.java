package org.naixue.mazh.flink1142.source.builtin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 从本地集合读取数据，进行计算
 */
public class FlinkSource_FromCollection {

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

        // TODO_MA 马中华 注释：
        SingleOutputStreamOperator<String> resultDS = dataSourceDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "nx_" + s;
            }
        });

        // TODO_MA 马中华 注释：
        resultDS.print();

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("FlinkSource_FromCollection");
    }
}
