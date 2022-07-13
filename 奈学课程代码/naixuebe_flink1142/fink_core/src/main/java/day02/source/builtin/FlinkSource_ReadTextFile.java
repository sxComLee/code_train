package day02.source.builtin;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 读取 HDFS 文件进行演示
 */
public class FlinkSource_ReadTextFile {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        // TODO_MA 马中华 注释： 具体从哪里读取数据，根据 schema 来决定： file:///   hdfs://
        // TODO_MA 马中华 注释： 读取本地文件
        // TODO_MA 马中华 注释： 这个东西和 spark 一样，也都是通过 跟 Hadoop 兼容的 InputFormat 去读取的
        // TODO_MA 马中华 注释： 决定到底怎么读的规则，InputFormat 决定的
        DataSource<String> lineDS = executionEnvironment.readTextFile("file:///c:/bigdata-data/wc/input/wordcount.txt");
        // TODO_MA 马中华 注释： 读取 HDFS 文件，需要准备 core-site.xml 和 hdfs-site.xml 文件放置到项目中
        // DataSource<String> lineDS = executionEnvironment.readTextFile("/wc/input/words.txt");

        // TODO_MA 马中华 注释：
        lineDS.print();

        // TODO_MA 马中华 注释： 异常：No new data sinks have been defined since the last execution.
        // TODO_MA 马中华 注释： The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
        // executionEnvironment.execute("FlinkSource_ReadTextFile");
    }
}
