package org.naixue.mazh.flink1142.partitioner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.PartitionOperator;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 关于 Flink 批处理的 分区策略有那些呢？
 *  1、rebalance
 *  2、partitionByHash
 *  3、partitionByRange
 *  4、partitionByCustom
 */
public class FlinkBatchPartitioner_PartitionByHash {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 1、获取编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 2、构造数据
        DataSource<Integer> dataset = executionEnvironment.fromElements(
                1, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7, 4, 5, 6, 7);

        // TODO_MA 马中华 注释： 3、Hash散列，Hash分区
        PartitionOperator<Integer> resultDS = dataset.partitionByHash(i -> i).setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,5,6,5,6,5,6)
         *  2、(4,4,4)
         *  3、(2,3,3,7,7)
         *  根据上述结果可以看出来，相同的元素，的确都在同一个分区内。这就是 Hash 的规则
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_hash\\result1142");

        executionEnvironment.execute("FlinkBatchPartitioner_PartitionByHash");
    }
}
