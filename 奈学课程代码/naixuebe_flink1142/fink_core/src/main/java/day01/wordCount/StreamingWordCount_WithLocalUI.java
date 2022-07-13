package day01.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Flink-1.14 版本的 流式 wordcount 入门案例
 *  测试的时候，运行：nc -lk 6789
 *
 */
public class StreamingWordCount_WithLocalUI {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境对象 StreamExecutionEnvironment
        //        <dependency>
        //            <groupId>org.apache.flink</groupId>
        //            <artifactId>flink-runtime-web_2.11</artifactId>
        //            <version>${flink.version}</version>
        //        </dependency>
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //自定义端口
        conf.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        executionEnvironment.setParallelism(2);

        // TODO_MA 马中华 注释： 加载数据源获取数据抽象对象
        // TODO_MA 马中华 注释： 其实 DataStreamSource 就是一个 DataStream
        DataStreamSource<String> sourceDataStream = executionEnvironment.socketTextStream("bigdata02", 6789);

        // TODO_MA 马中华 注释： 执行逻辑处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = sourceDataStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(new Tuple2(word, 1));
                        }
                    }
                }).setParallelism(3);

        // TODO_MA 马中华 注释： 分组聚合
        // TODO_MA 马中华 注释： shuffle 算子肯定不会合并
        // TODO_MA 马中华 注释： 相邻两个可以合并，相邻三个：  A B C ==> AB C ==> ABC
        // TODO_MA 马中华 注释：                          A B C ==> A BC ==> ABC
        // TODO_MA 马中华 注释： MR, Spakr， Flink 中关于 shuffle 的处理设计思路，都是一样的
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneDS
                .keyBy(0).sum(1).setParallelism(4);

        // TODO_MA 马中华 注释： 结果输出
        resultDS.print().setParallelism(5);

        // TODO_MA 马中华 注释： 如果涉及到 operator 的合并 OperatorChain，那么你估算的 Task 总数，可能跟你的 UI 中的呈现不一样了。

        // TODO_MA 马中华 注释： 提交执行
        executionEnvironment.execute("Flink Streaming WordCount");

        // TODO_MA 马中华 注释： 相邻两个 Operator 要能合并，必须要满足9个条件
        // TODO_MA 马中华 注释： 其中有一个条件就是： 这两个算子的并行度要一样
    }
}
