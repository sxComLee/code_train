package org.lij.flink1_14_2.day02.source.builtin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-28 07:47
 */
public class FlinkSource_readFromFile {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        System.setProperty("HADOOP_USER_NAME","bigdata");
        // 具体从哪里读取数据，根据 schema 来决定： file:///   hdfs://
        // 读取本地文件
        // 这个东西和 spark 一样，也都是通过 跟 Hadoop 兼容的 InputFormat 去读取的
        // 决定到底怎么读的规则，InputFormat 决定的
        DataSource<String> fileSource = env.readTextFile("file:///Users/jiang.li/workspace/self/learnCode/flink-1.14/src/main/resources/lij_test.csv");

        // 读取 HDFS 文件，需要准备 core-site.xml 和 hdfs-site.xml 文件放置到项目中
        // DataSource<String> lineDS = executionEnvironment.readTextFile("/wc/input/words.txt");

        FlatMapOperator<String, Object> mapSource = fileSource.flatMap((line, collector) -> {
            String[] words = line.split(",");
            for (String word : words
            ) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }).setParallelism(2);

        mapSource.print();

        // 注释： 异常：No new data sinks have been defined since the last execution.
        // 注释： The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
        // env.execute("flink readFile");


    }
}
