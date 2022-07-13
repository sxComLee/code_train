package com.jiang.flink.program;

import com.jiang.flink.study.template.FlinkFromKafkaModule;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @ClassName TopN
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-21 11:08
 * @Version 1.0
 */
public class TopN extends FlinkFromKafkaModule {


    @Override
    public SingleOutputStreamOperator dealStream(DataStreamSource<String> kafkaSource, StreamExecutionEnvironment env) {
        kafkaSource.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)));
//                .process(new TopNAllFunction(5));
        return null;
    }



}

class TopNAllFunction
        extends
        ProcessAllWindowFunction<String, Tuple2<String, Integer>, TimeWindow> {

    private int topSize = 10;

    public TopNAllFunction(int topSize) {
        // TODO Auto-generated constructor stub

        this.topSize = topSize;
    }

    @Override
    public void process(
            ProcessAllWindowFunction<String, Tuple2<String, Integer>, TimeWindow>.Context arg0,
            Iterable<String> input,
            Collector<Tuple2<String, Integer>> out) throws Exception {
        // TODO Auto-generated method stub

        TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
                new Comparator<Integer>() {

                    @Override
                    public int compare(Integer y, Integer x) {
                        // TODO Auto-generated method stub
                        return (x < y) ? -1 : 1;
                    }

                }); //treemap按照key降序排列，相同count值不覆盖

//        for (String element : input) {
//            treemap.put(element.f1, element);
//            if (treemap.size() > topSize) { //只保留前面TopN个元素
//                treemap.pollLastEntry();
//            }
//        }

        for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
                .entrySet()) {
            out.collect(entry.getValue());
        }

    }

}
