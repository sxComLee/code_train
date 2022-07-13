package com.jiang.flink.study.window;


import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName GroupPressingWindow
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-10-30 11:38
 * @Version 1.0
 */
public class GroupedProcessingTimeWindowExample {

    public static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
            Random random = new Random();
            while (isRunning) {
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(3);
                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                sourceContext.collect(new Tuple2<>(key, value));
                //产生timestamp和watermark
                sourceContext.collectWithTimestamp(new Tuple2<>(key, value),System.currentTimeMillis());
                if(System.currentTimeMillis()%5==0){
                    //每隔5秒产生一个watermark
                    sourceContext.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception{
        //创建配置信息
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();
        //创建环境变量
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Tuple2<String, Integer>> ds = env.addSource(new DataSource());

        //设置waterMark，定时产生waterMark
        ds.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - 5000);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2, long l) {
                return 0;
            }
        });
//        //根据特殊记录产生waterMark
//        ds.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, Integer>>() {
//            @Nullable
//            @Override
//            public Watermark checkAndGetNextWatermark(Tuple2<String, Integer> lastElement, long extractedTimestamp) {
//                return null;
//            }
//
//            @Override
//            public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
//                return 0;
//            }
//        });


        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        int evictionSec = 10;

        keyedStream.window(GlobalWindows.create())
                .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                //设置  `allowedLateness` 之后，迟来的数据同样可以触发窗口，进行输出，利用 Flink 的 side output 机制，我们可以获取到这些迟到的数据
                .allowedLateness(Time.milliseconds(12L))
                //指定迟到数据的标签，迟到的数据也会触发窗口
                .sideOutputLateData(new OutputTag<>("late-data"));

        //状态


        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> stringIntegerHashMap, Tuple2<String, Integer> o) throws Exception {
                stringIntegerHashMap.put(o.f0, o.f1);
                return stringIntegerHashMap;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value) throws Exception {
                System.out.println(value);
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });


        env.execute();
    }
}
