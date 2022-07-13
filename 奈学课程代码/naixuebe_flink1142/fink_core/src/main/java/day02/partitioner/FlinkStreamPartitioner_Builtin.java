package day02.partitioner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flink 的流分区详解
 */
public class FlinkStreamPartitioner_Builtin {

    public static void main(String[] args) {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> sourceDS = executionEnvironment.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 1、伸缩：1 => N, 或者 N => 1
        DataStream<String> rescaleDatastream = sourceDS.rescale();

        // TODO_MA 马中华 注释： 2、重新平衡分区: 均匀， Round-Robin算法
        DataStream<String> rebalanceDatastream = sourceDS.rebalance();

        // TODO_MA 马中华 注释： 3、keyBy 根据指定的字段来进行 hash 分区，相同的key 必定在同一分区内
        DataStream<String> keyedDatastream = sourceDS.keyBy(1);

        // TODO_MA 马中华 注释： 4、所有输出数据，都发送给下游第一个 Task
        DataStream<String> globalDatastream = sourceDS.global();

        // TODO_MA 马中华 注释： 5、上下游的本地 Task 映射
        DataStream<String> forwardDatastream = sourceDS.forward();

        // TODO_MA 马中华 注释： 6、随机分区: 根据均匀分布随机分配元素。
        DataStream<String> suffleDatastream = sourceDS.shuffle();

        // TODO_MA 马中华 注释： 7、广播：向每个分区广播元素，把所有元素广播到所有分区
        DataStream<String> broadcastDatastream = sourceDS.broadcast();

    }
}
