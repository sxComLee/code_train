package com.jiang.flink.study.operate;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName chain
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-20 16:06
 * @Version 1.0
 */
public class chain {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        //通过这个命令可以禁止链式操作，全局禁止链式操作
        env.disableOperatorChaining();

        env.fromElements(parameterTool.get("example.words").split("\\t"))
                //在source操作或者operator操作或者sink操作上执行
                .slotSharingGroup("test")
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");
                        for (String split : splits) {
                            out.collect(new Tuple2<>(split, 1));
                        }
                    }
                })
                .slotSharingGroup("test")
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        return value;
                    }
                })
                .startNewChain()    //start the new chain，flatMap & map operator will chain, but not reduce
                //通过这行代码，会在此处断开链式操作
                .disableChaining()
                .keyBy(0)
                .reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .slotSharingGroup("test")
                .print()
                .slotSharingGroup("test");

        //get the job ExecutionPlan json
        System.out.println("=====" + env.getExecutionPlan());

        env.execute();

    }
}
