package com.jiang.flink.study.operate;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName MapAndFlatMapAndFilter
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-16 15:14
 * @Version 1.0
 */
public class BasicOperate {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        String hostName = "localhost";
        int port = 8001;
        DataStreamSource<String> testSource = env.socketTextStream(hostName, port);


//        SingleOutputStreamOperator<String> map = testSource.map(x -> "hello"+x);
//
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = testSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");
                for (String line : tokens) {
                    Random random = new Random();
                    int i = random.nextInt(10);
                    System.out.println("当前数据为" + i);
                    collector.collect(new Tuple2<>(line, i));
                }
            }
        });
//        flatMap.print();
//
//        SingleOutputStreamOperator<String> filter = testSource.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                if (s.equals("hello")) {
//                    return true;
//                }
//                return false;
//            }
//        });

        JoinedStreams.WithWindow<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> window1
                = flatMap.join(flatMap)
                //指定第一个流的关联字段
                .where(x -> "")
                //指定第二个流的关联字段
                .equalTo(x -> "")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

//        //根据Tuple的第一个元素进行分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
//        sum.print();

//        SingleOutputStreamOperator<Tuple2<String, Integer>> min = keyedStream.min(1);
//        min.print();
//        SingleOutputStreamOperator<Tuple2<String, Integer>> minBy = keyedStream.minBy(1);
//        minBy.print();


//        keyedStream.print();
//        //根据javaBean的属性name进行分区
//        testSource.keyBy("");
//        //自定义返回的key值，并指定key的序列化类型
//        testSource.keyBy(new KeySelector<String, String>() {
//            @Override
//            public String getKey(String s) throws Exception {
//                return "";
//            }
//        }, BasicTypeInfo.STRING_TYPE_INFO);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//                return new Tuple2<>(t1.f0,t1.f1+t2.f1);
//            }
//        });
//        reduce.print();

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window
                = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));



//        window.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value, Tuple2<String, Integer> t1) throws Exception {
//                return new Tuple2<>(value.f0,value.f1+t1.f1);
//            }
//        });

//        window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {
//            @Override
//            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
//                int sum = 0;
//                for (Tuple2<String, Integer> t : elements) {
//                    sum += t.f1;
//                }
//                out.collect(sum);
//            }
//        });
//
//        window.apply(new WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
//                int sum = 0;
//                for (Tuple2<String, Integer> t : input) {
//                    sum += t.f1;
//                }
//                out.collect(sum);
//            }
//        });


        env.execute();

    }
}
