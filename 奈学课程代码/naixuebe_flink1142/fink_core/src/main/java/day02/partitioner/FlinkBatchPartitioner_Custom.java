package day02.partitioner;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义 分区规则实现 数据分区
 */
public class FlinkBatchPartitioner_Custom {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 构造编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 构造数据
        DataSet<Integer> sourceDS = executionEnvironment.fromElements(1, 2, 3, 4, 5, 11, 22, 33, 44, 55, 66, 111, 222, 333, 444,
                555, 666
        );

        // TODO_MA 马中华 注释： 调用已定义的分区规则
        // TODO_MA 马中华 注释： Partitioner_Batch
        PartitionOperator<Integer> resultDS = sourceDS.partitionCustom(new Partitioner_Batch(), word -> word).setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,2,3,4,5)
         *  2、(11,22,33,44,55,66)
         *  3、(11,222,333,444,555,666)
         *  根据自定义逻辑：0好分区的数值小于10， 1号分区的数据小于100，剩下的在2号分区，总共三个分区
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_custom\\result");

        // TODO_MA 马中华 注释： 提交
        executionEnvironment.execute("FlinkBatchPartitioner_Custom");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  1、partition 方法的作用，就是用给定的 值 和 逻辑计算得到这个 值应该是在那个分区里面
     *  2、参数 key，待计算的值
     *  3、参数 numPartitions， 总分区数
     *  4、int 方法返回值，代表这个 key 在那个分区
     */
    public static class Partitioner_Batch implements Partitioner<Integer> {

        @Override
        public int partition(Integer value, int numPartitions) {
            int ptn_index = 0;
            if (value < 10) {
                ptn_index = 0;
            } else if (value < 100) {
                ptn_index = 1;
            } else {
                ptn_index = 2;
            }
            return ptn_index;
        }
    }
}
