package com.jiang.flink.study.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @ClassName BlinkBatchQuery
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-09-25 12:24
 * @Version 1.0
 */
public class BlinkBatchQuery {
    public static void main(String[] args) throws Exception{
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
        bbTableEnv.sqlUpdate("");
        bbTableEnv.execute("");
    }
}
