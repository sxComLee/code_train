package com.jiang.flink.study.cep;

import com.jiang.flink.study.common.util.ExecutionEnvUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName CepTesgt
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-23 14:51
 * @Version 1.0
 */
public class CepTest {
    public static void main(String[] args) throws Exception{

        DataStream<String> testSource = ExecutionEnvUtil.getTestSource();

        StreamExecutionEnvironment env = testSource.getExecutionEnvironment();

        testSource.print();

        env.execute();

//        Pattern.<LogEvent>begin("start").where(new SimpleCondition<LogEvent>() {
//            @Override
//            public boolean filter(LogEvent value) throws Exception {
//                return value.getType() == "12";
//            }
//        }).next("middle").subtype()
    }
}
