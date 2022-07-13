package com.jiang.flink.study.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.flink.table.api.java.StreamTableEnvironment;


/**
 * @ClassName BlinkStreamingQuery
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-09-25 12:24
 * @Version 1.0
 */
public class BlinkStreamingQuery{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment
//        TableEnvironment.create(bsSettings);

        bsTableEnv.sqlUpdate("");
        bsTableEnv.execute("");

    }
}
