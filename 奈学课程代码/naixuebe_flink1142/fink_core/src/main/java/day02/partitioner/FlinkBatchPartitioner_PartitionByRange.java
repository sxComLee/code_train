package day02.partitioner;

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
public class FlinkBatchPartitioner_PartitionByRange {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 1、获取编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 2、构造数据
        DataSource<Integer> dataset = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7, 4, 5, 6, 7);

        // TODO_MA 马中华 注释： 3、Hash散列，Hash分区
        PartitionOperator<Integer> resultDS = dataset.partitionByRange(i -> i).setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,2,3,4,3,4,4)
         *  2、(5,6,5,6,5,6)
         *  3、(7,7)
         *  根据上述结果，很明显，分成了3个分段，第一段小于第二段，第二段小于第三段
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_range\\result");

        executionEnvironment.execute("FlinkBatchPartitioner_PartitionByHash");
    }
}
