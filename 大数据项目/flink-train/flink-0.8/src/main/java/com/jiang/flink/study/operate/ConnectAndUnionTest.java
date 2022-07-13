package com.jiang.flink.study.operate;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName ConnectTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-11 09:12
 * @Version 1.0
 */
public class ConnectAndUnionTest {

    public static final String[] WORDS = new String[] {
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;"
            ,"And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };

    
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Long> someStream = env.generateSequence(0, 10);
        DataStreamSource<String> otherStream = env.fromElements(WORDS);

        ConnectedStreams<Long, String> connect = someStream.connect(otherStream);

        SingleOutputStreamOperator<String> result = connect.flatMap(new CoFlatMapFunction<Long, String, String>() {
            @Override
            public void flatMap1(Long aLong, Collector<String> collector) throws Exception {
                collector.collect(aLong.toString());
            }

            @Override
            public void flatMap2(String s, Collector<String> collector) throws Exception {
                for (String word : s.split("\\W+")) {
                    collector.collect(word);
                }
            }
        });

        result.print();
        env.execute("test connect");
    }


}
