package org.naixue.mazh.flink1142.partitioner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flink 的流分区详解
 *  Flink 的流式编程中，内置了 8 大分区策略，几乎完全够用！
 */
public class FlinkStreamPartitioner_Builtin {

    public static void main(String[] args) {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释：
        DataStreamSource<String> sourceDS = executionEnvironment.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 1、伸缩：1 => N, 或者 N => 1
        // TODO_MA 马中华 注释： 上下游的并行度不一样，怎么去shuffle数据
        DataStream<String> rescaleDatastream = sourceDS.rescale();

        // TODO_MA 马中华 注释： 2、重新平衡分区: 均匀， Round-Robin算法  轮询
        DataStream<String> rebalanceDatastream = sourceDS.rebalance();

        // TODO_MA 马中华 注释： 3、keyBy 根据指定的字段来进行 hash 分区，相同的 key 必定在同一分区内
        // TODO_MA 马中华 注释： 按照指定的 key 来做 hash
        // TODO_MA 马中华 注释： MR 分成两个阶段： 第一个mapper阶段：提取待计算的数据，打上标记：key
        // TODO_MA 马中华 注释： MR 分成连个阶段： 第二个reducer阶段：获取到了key相同的一组待计算的数据，执行聚合计算
        DataStream<String> keyedDatastream = sourceDS.keyBy(1);

        // TODO_MA 马中华 注释： 4、所有输出数据，都发送给下游第一个 Task
        DataStream<String> globalDatastream = sourceDS.global();

        // TODO_MA 马中华 注释： 5、上下游的本地 Task 映射
        // TODO_MA 马中华 注释： 注意一个知识： 上下游两个 Operator 之间能否链接成 Operator Chain 有 9 个条件
        DataStream<String> forwardDatastream = sourceDS.forward();

        // TODO_MA 马中华 注释： 6、随机分区: 根据均匀分布随机分配元素。
        DataStream<String> suffleDatastream = sourceDS.shuffle();

        // TODO_MA 马中华 注释： 7、广播：向每个分区广播元素，把所有元素广播到所有分区
        DataStream<String> broadcastDatastream = sourceDS.broadcast();

    }
}
