package com.jiang.flink.study.scala.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jiang.flink.study.sink.HbaseSink;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @ClassName SocketTextStreamWordCount
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-09-20 09:28
 * @Version 1.0
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        //参数检查
//        if(args.length != 2){
//            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
//            return;
//        }

        String hostName = "localhost";
        int port = Integer.parseInt("8001");

        //set up the streaming execution evironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> testStream = env.socketTextStream(hostName, port);

        //计数 keyed state
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = testStream.flatMap(new LineSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(1))
                .sum(1);
        sum.print();


        testStream.flatMap(new mapToHbase())
                .addSink(new HbaseSink("test", "d"));




        env.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    public static final class mapToHbase implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            JSONArray ja = new JSONArray();
            for (String token : tokens) {
                JSONObject jb = new JSONObject();
                if (token.length() > 0) {
                    JSONObject jc = new JSONObject();
                    jc.put(value, value + "_value");
                    jb.put("rowkey_" + value, jc);
                }
                ja.add(jb);
            }
            out.collect(ja.toJSONString());
        }
    }

}



