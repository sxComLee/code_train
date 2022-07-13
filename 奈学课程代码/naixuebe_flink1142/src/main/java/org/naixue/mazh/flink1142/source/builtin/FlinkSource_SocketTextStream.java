package org.naixue.mazh.flink1142.source.builtin;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Wordcount 程序，就是通过 socketTextStream 方式来读取数据的
 */
public class FlinkSource_SocketTextStream {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

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
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneDS.keyBy(0).sum(1).setParallelism(4);

        // TODO_MA 马中华 注释： 结果输出
        resultDS.print().setParallelism(5);

        // TODO_MA 马中华 注释： 提交执行
        executionEnvironment.execute("FlinkSource_SocketTextStream");
    }
}
