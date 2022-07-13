package org.naixue.mazh.flink1142.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义 Partitioner
 */
public class FlinkStreamPartitioner_Custom {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> sourceDS = executionEnvironment.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 调用自定义的 分区器
        DataStream<String> resultDS = sourceDS.partitionCustom(new CustomPartitioner(), word -> word);

        // TODO_MA 马中华 注释：
        resultDS.print().setParallelism(1);

        // TODO_MA 马中华 注释：
        executionEnvironment.execute("FlinkStreamPartitioner_Custom");
    }

    // TODO_MA 马中华 注释： 自定义一个 分区器
    public static class CustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}

