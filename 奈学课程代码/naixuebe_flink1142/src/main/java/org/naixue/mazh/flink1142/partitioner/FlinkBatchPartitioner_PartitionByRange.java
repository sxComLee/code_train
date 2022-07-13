package org.naixue.mazh.flink1142.partitioner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.PartitionOperator;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 关于 Flink 批处理的 分区策略有那些呢？
 *  1、rebalance
 *  2、partitionByHash
 *  3、partitionByRange 范围分区 （1-3,4-5,6-7）
 *  4、partitionByCustom
 */
public class FlinkBatchPartitioner_PartitionByRange {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 1、获取编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 2、构造数据
        DataSource<Integer> dataset = executionEnvironment.fromElements(
                1, 2, 3, 4, 5, 6, 3, 3, 4, 5, 6, 3, 4, 5, 6, 7, 4, 5, 6, 7);

        // TODO_MA 马中华 注释： 3、Hash散列，Hash分区
        PartitionOperator<Integer> resultDS = dataset.partitionByRange(i -> i).setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,2,3,4,3,4,4)   100W
         *  2、(5,6,5,6,5,6)   101W
         *  3、(7,7)     98W
         *  根据上述结果，很明显，分成了3个分段，第一段小于第二段，第二段小于第三段
         *  -
         *  假设 1-10000 条数据：
         *  1-3333
         *  3334 - 6667
         *  6668 - 10000
         *  -
         *  范围分区的作用： 基于这个范围分区，来实现全局排序！
         *  -
         *  如何实现全局排序呢？ 求解某个数据集中的最小的 3 个值
         *  1、先做范围分区
         *  2、每个分区内做排序（升序排序）
         *  3、取前面 3 条数据
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_range\\result1142");

        executionEnvironment.execute("FlinkBatchPartitioner_PartitionByHash");
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 分区的常见几种手段
 */

