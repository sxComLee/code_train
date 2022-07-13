package org.naixue.mazh.flink1142.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.PartitionOperator;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义 分区规则实现 数据分区
 */
public class FlinkBatchPartitioner_Custom {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 构造编程入口
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 构造数据
        DataSet<Integer> sourceDS = executionEnvironment.fromElements(
                1, 2, 3, 4, 5, 11, 22, 33, 44, 55, 66, 111, 222, 333, 444,
                555, 666
        );

        // TODO_MA 马中华 注释： 调用已定义的分区规则
        // TODO_MA 马中华 注释： Partitioner_Batch
        PartitionOperator<Integer> resultDS = sourceDS
                .partitionCustom(new Partitioner_Batch(), word -> word).setParallelism(3);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 由于设置了并行度为3，所以最终会生成 3 个文件，分别表示三个分区的数据：
         *  1、(1,2,3,4,5)
         *  2、(11,22,33,44,55,66)
         *  3、(111,222,333,444,555,666)
         *  根据自定义逻辑：0好分区的数值小于10， 1号分区的数据小于100，剩下的在2号分区，总共三个分区
         */
        resultDS.writeAsText("file:///C:\\bigdata-data\\flink_batch_custom\\result1142");

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
     *  -
     *  MR，Spark，Flink 都是这么干的！
     */
    public static class Partitioner_Batch implements Partitioner<Integer> {

        // TODO_MA 马中华 注释： 分三个分区
        // TODO_MA 马中华 注释： < 10 的分到第 1 个 Task
        // TODO_MA 马中华 注释： < 100 的分到第 2 个 Task
        // TODO_MA 马中华 注释： >= 100 的分到第 3 个 Task
        // TODO_MA 马中华 注释： 第一个参数：待分区的元素，第二个参数：分区的总个数
        // TODO_MA 马中华 注释： 方法返回值；代表 待分区元素经过方法的逻辑计算得到了一个分区的编号
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
        // TODO_MA 马中华 注释： numPartitions 如果等于 3， 则 分区编号只能是 0,1,2 不能是其他值

        // TODO_MA 马中华 注释： 手机号 131 开头的放到第一个 分区
        // TODO_MA 马中华 注释： 手机号 132 开头的放到第二个分区
    }
}
