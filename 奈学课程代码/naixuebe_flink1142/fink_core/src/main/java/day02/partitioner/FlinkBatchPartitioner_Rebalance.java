package day02.partitioner;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 关于 Flink 批处理的 分区策略有那些呢？
 *  1、rebalance
 *  2、partitionByHash
 *  3、partitionByRange
 *  4、partitionByCustom
 */
public class FlinkBatchPartitioner_Rebalance {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 构造数据源
        DataSource<Integer> dataset = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 14);

        // TODO_MA 马中华 注释： 重新平衡。解决数据倾斜，采用 Round-Robin 的方式
        PartitionOperator<Integer> resultDS = dataset.rebalance().setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,4,7,10,13)
         *  2、(2,5,8,11,15)
         *  3、(3,6,9,12,14)
         *  注意，我故意调换了 14 和 15 的位置，可以验证出来的确时候按照 轮询 来分配的
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_reblance\\result");

        executionEnvironment.execute("FlinkBatchPartitioner_PartitionByHash");
    }
}
