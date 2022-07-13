package org.lij.flink1_14_2.day01.wordCount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.configuration.ConfigConstants.LOCAL_START_WEBSERVER;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-24 23:01
 */
public class StreamingWordCount_WithLocalUI {
    public static void main(String[] args) throws Exception{
        // TODO_MA 马中华 注释： 获取执行环境对象 StreamExecutionEnvironment
        //        <dependency>
        //            <groupId>org.apache.flink</groupId>
        //            <artifactId>flink-runtime-web_2.11</artifactId>
        //            <version>${flink.version}</version>
        //        </dependency>
        Configuration conf = new Configuration();
        conf.setBoolean(LOCAL_START_WEBSERVER,true);
        // 自定义端口号
        conf.setInteger(RestOptions.PORT,6789);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStream<String> source = env.socketTextStream("localhost", 6789);

        SingleOutputStreamOperator<Object> wordOnes = source.flatMap((line, collector) -> {
            String[] words = line.split(" ");
            for (String word : words
            ) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }).setParallelism(3);

        // 分组聚合，shufle算子不会合并成 operatorChain
        SingleOutputStreamOperator<Object> resultDS = wordOnes.keyBy(0).sum(1).setParallelism(4);

        // 结果输出
        resultDS.print().setParallelism(5);
        // 如果涉及到operator 的合并 operatorChain，一个task处理operatorChain ，那么估算的task 总数，可能和webUI中呈现的不一样
        env.execute("flink streaming wordCount localWebUI");

        // 相邻两个 Operator 合并需要满足9个条件，其中一个就是两个算子的并行度必须相同
    }
}
