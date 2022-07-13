package org.lij.flink1_14_2.day02.source.builtin;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-28 08:01
 */
public class FlinkSource_SocketTextStream {
    public static void main(String[] args) throws Exception {

        // 获取执行环境对象 StreamExecutionEnvironment
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 加载数据源获取数据抽象对象
        // 其实 DataStreamSource 就是一个 DataStream
        // 所有的经过算子转换的结果都是一个 SingleOutPutStreamOperator，是 DataStream的子类
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
