package org.lij.flink1_14_2.day02.source.builtin;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-27 09:45
 */
public class FlinkSource_FromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 创建collection 容器
        ArrayList<String> data = new ArrayList<>();
        data.add("flink");
        data.add("spark");
        data.add("mr");
        data.add("storm");

        DataStreamSource<String> dataStreamSource = env.fromCollection(data);

        SingleOutputStreamOperator<String> mapDS = dataStreamSource.map(a -> "nx" + a);

        mapDS.print();

        env.execute("flink stream source_fromCollection");
    }
}
