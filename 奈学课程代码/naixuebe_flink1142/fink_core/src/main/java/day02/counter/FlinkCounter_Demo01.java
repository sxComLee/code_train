package day02.counter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flink 累加器执行 测试
 */
public class FlinkCounter_Demo01 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： 构造 DataSet
        // TODO_MA 马中华 注释： 比如现在做 ETL
        // TODO_MA 马中华 注释： 所有字段都存在合法值的记录条数：a
        // TODO_MA 马中华 注释： 缺失一个字段值的记录条数： b
        // TODO_MA 马中华 注释： 缺失二个字段值的记录条数： c
        // TODO_MA 马中华 注释： total = a + b + c
        DataSource<String> data = env.fromElements("a", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d");

        // TODO_MA 马中华 注释： spark flink 计算引擎
        // TODO_MA 马中华 注释： 每一条数据，都得参与计算！
        // TODO_MA 马中华 注释： 使用 map 的出发点，其实就是要对 dataset 中的每一个元素，执行一次计算的

        // TODO_MA 马中华 注释： 执行计算
        // TODO_MA 马中华 注释： 这个 map 方法的 匿名对象 参数 是每个 Task 一个
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            // TODO_MA 马中华 注释： 1、创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // TODO_MA 马中华 注释： 2、注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
                // TODO_MA 马中华 注释： 在逻辑上来说，相当于在这个 application 的内部定义了一个变量 num-lines 用来做统计
                // TODO_MA 马中华 注释： 但是，物理上，其实这个 num-lines 变量是由分散在所有 Task 内部的 numLines 组成的
                // TODO_MA 马中华 注释： 一个 num-lines 包含了很多个 numLines
                // TODO_MA 马中华 注释： 其实最终拿到的结果，就是把 所有 Task 中的 numLines 加起来，就是 num-lines 的值
            }
            // TODO_MA 马中华 注释： Flink 流编程有 累加器么? Flink State 编程

            //int sum = 0;
            @Override
            public String map(String value) throws Exception {
                //如果并行度为1，使用普通的累加求和即可，但是设置多个并行度，则普通的累加求和结果就不准了
                //sum++;
                //System.out.println("sum："+sum);
                if (!value.equals("a")){   // TODO_MA 马中华 注释： a 不参与统计
                    this.numLines.add(1);
                }
                return value;
            }
        }).setParallelism(8);

        // TODO_MA 马中华 注释： 输出计算结果
        result.writeAsText("file:///C:\\bigdata-data\\flink_counter\\result22");

        // TODO_MA 马中华 注释： 提交应用程序执行
        JobExecutionResult jobResult = env.execute("FlinkCounter_Demo01");

        // TODO_MA 马中华 注释： 3、获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:" + num);
    }
}
