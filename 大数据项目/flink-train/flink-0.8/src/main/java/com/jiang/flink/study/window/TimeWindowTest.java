package com.jiang.flink.study.window;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import com.jiang.flink.study.template.FlinkFromKafkaModule;
import com.jiang.flink.study.window.function.LineSplitter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName TimeWindowTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-27 09:15
 * @Version 1.0
 */
public class TimeWindowTest extends FlinkFromKafkaModule {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        String HOST_NAME = "host.name";
        String PORT = "port";
        DataStreamSource<String> data = env.socketTextStream(parameterTool.get(HOST_NAME), parameterTool.getInt(PORT));


        //基于时间窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                //关于window有三种，一种是timewindow，一种是countwindow 还有一种是两个都包含的window
                .timeWindow(Time.seconds(30))
                .sum(0)
                .print();
        //基于滑动时间窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                //每隔p2秒，对p1秒之前的数据进行计算
                .timeWindow(Time.seconds(30),Time.seconds(15))
                .sum(0)
                .print();

        //基于事件数量窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                //每隔p2秒，对p1秒之前的数据进行计算
                .countWindow(3)
                .sum(0)
                .print();

        //基于事件滑动窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                //每隔p2秒，对p1秒之前的数据进行计算
                .countWindow(3,2)
                .sum(0)
                .print();

        //基于会话的事件窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                //表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();

        data.flatMap(new LineSplitter())
                .keyBy(1)
                //表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .window(GlobalWindows.create());

        env.execute("flink window example");
    }

    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
        //keyedStream的操作
        kafkaSource.keyBy(0)
                .window(new WindowAssigner<String, Window>() {
                    @Override
                    public Collection<Window> assignWindows(String element, long timestamp, WindowAssignerContext context) {
                        return null;
                    }

                    @Override
                    public Trigger<String, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
                        return null;
                    }

                    @Override
                    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                        return null;
                    }

                    @Override
                    public boolean isEventTime() {
                        return false;
                    }
                })
                .trigger(new Trigger<String, Window>() {
                    @Override
                    public TriggerResult onElement(String element, long timestamp, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(Window window, TriggerContext ctx) throws Exception {

                    }
                })
                .evictor(new Evictor<String, Window>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, Window window, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, Window window, EvictorContext evictorContext) {

                    }
                })
                .allowedLateness(Time.of(10L, TimeUnit.DAYS))
                .sideOutputLateData(new OutputTag<>("tag"))
                .aggregate(new AggregateFunction<String, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return null;
                    }

                    @Override
                    public String add(String value, String accumulator) {
                        return null;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return null;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return null;
                    }
                })
//                .reduce(new ReduceFunction<String>() {
//                    @Override
//                    public String reduce(String value1, String value2) throws Exception {
//                        return null;
//                    }
//                })
                .getSideOutput(new OutputTag<>(""))
        ;

//非keyedStream的操作
        kafkaSource
                .windowAll(new WindowAssigner<String, Window>() {
                    @Override
                    public Collection<Window> assignWindows(String element, long timestamp, WindowAssignerContext context) {
                        return null;
                    }

                    @Override
                    public Trigger<String, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
                        return null;
                    }

                    @Override
                    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                        return null;
                    }

                    @Override
                    public boolean isEventTime() {
                        return false;
                    }
                })
                .trigger(new Trigger<String, Window>() {
                    @Override
                    public TriggerResult onElement(String element, long timestamp, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(Window window, TriggerContext ctx) throws Exception {

                    }
                })
                .evictor(new Evictor<String, Window>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, Window window, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, Window window, EvictorContext evictorContext) {

                    }
                })
                .allowedLateness(Time.of(10L, TimeUnit.DAYS))
                .sideOutputLateData(new OutputTag<>("tag"))
                .aggregate(new AggregateFunction<String, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return null;
                    }

                    @Override
                    public String add(String value, String accumulator) {
                        return null;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return null;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return null;
                    }
                })
//                .reduce(new ReduceFunction<String>() {
//                    @Override
//                    public String reduce(String value1, String value2) throws Exception {
//                        return null;
//                    }
//                })
                .getSideOutput(new OutputTag<>(""))
        ;

        return null;
    }


}
