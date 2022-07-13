package org.lij.flink1_14_2.day02.counter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

/**
 * Description:
 *  flink 累加器测试
 * @author lij
 * @date 2022-01-11 20:25
 */
public class FlinkCounter_Demo01 {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.fromElements("a", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d");

        // 8
        System.out.println("获取默认的并行度。。。。"+env.getParallelism());
        // 1
        System.out.println("获取source默认的并行度。。。。"+source.getParallelism());

        // operate 算子进行计算
        MapOperator<String, String> map = source.map(new RichMapFunction<String, String>() {

            //注册int累加器
            private IntCounter counter =  new IntCounter();

            @Override
            public void open(Configuration conf) throws Exception {
                super.open(conf);
                // 添加累加器
                getRuntimeContext().addAccumulator("num-line",this.counter);
            }

            @Override
            public String map(String word) throws Exception {
                if(!"a".equals(word)){
                    this.counter.add(1);
                }
                return word;
            }
        }).setParallelism(3);

        System.out.println("获取map算子的并行度。。。。"+map.getParallelism());

    //    结果输出
        map.writeAsText("result.csv",OVERWRITE).setParallelism(1);

    //    程序执行
        JobExecutionResult jobResult = env.execute("flinkCounter");

        int num = jobResult.getAccumulatorResult("num-line");

        System.out.println("累加器结果 num ："+num);

    }
}
