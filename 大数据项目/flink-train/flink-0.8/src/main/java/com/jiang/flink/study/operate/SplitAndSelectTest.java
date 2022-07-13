package com.jiang.flink.study.operate;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
// import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
// import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName SplitAndSelectTest
 * @Description TODO 已经被标注为过期，建议使用side outputs
 * @Author jiang.li
 * @Date 2019-12-11 09:23
 * @Version 1.0
 */
public class SplitAndSelectTest {
    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Long> input = env.generateSequence(0, 10);
        // SplitStream<Long> split = input.split(new OutputSelector<Long>() {
        //     @Override
        //     public Iterable<String> select(Long value) {
        //         List<String> output = new ArrayList<String>();
        //         if (value % 2 == 0) {
        //             output.add("even");
        //         } else {
        //             output.add("odd");
        //         }
        //         return output;
        //     }
        // });
        // split.print();
        //
        // DataStream<Long> even = split.select("even");
        // DataStream<Long> odd = split.select("odd");
        // DataStream<Long> all = split.select("even","odd");
        //
        // even.print();
        // odd.print();
        // all.print();



        env.execute();


    }
}
